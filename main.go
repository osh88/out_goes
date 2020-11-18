package main

import "C"
import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/valyala/fasthttp"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func main() {}

var rnd = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// Контексты для разных экземпляров
// (в конфиге можно объявить несколько фильтров одного типа).
var contexts = make(map[string]*Ctx)

// Паттерн для извлечения параметров из шаблона имени индекса.
var indexPattern = regexp.MustCompile(`{([a-zA-Z_0-9#]*)}`)

// Представляет собой параметр в шаблоне имени индекса.
type indexField struct {
	Replace     string // Текст для замены в шаблоне (имя поля в фигурных скобках).
	FieldName   string // Имя поля.
	IsTimestamp bool   // Поле является отметкой времени события.
}

// Ctx контекст для конкретного экземпляра.
type Ctx struct {
	url         string       // Адрес эластика.
	index       string       // Шаблон имени индекса.
	parsedIndex []indexField // Разобранные параметры шаблона имени индекса.
	bulkSize    int          // Максимальный размер пачки для отправки в эластик.
	timeout     int          // Максимальное время ожидания обработки запроса эластиком (в секундах).
	debug       int          // Режим для отладки (0-выключен 1,2-вывод отладочной информации).
}

// Представляет собой единицу данных (запись) fluentbit.
type record map[string]interface{}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	return output.FLBPluginRegister(def, "goes", "Golang elasticsearch plugin!")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Gets called only once for each instance you have configured.
	var err error

	ctx := Ctx{}

	// Получаем адрес эластика.
	esAddr := output.FLBPluginConfigKey(plugin, "Addr")
	if esAddr == "" {
		esAddr = "localhost:9200"
	}
	ctx.url = fmt.Sprintf("http://%s/", esAddr)

	// Обрабатываем шаблон имени индекса.
	ctx.index = output.FLBPluginConfigKey(plugin, "Index")
	for _, list := range indexPattern.FindAllStringSubmatch(ctx.index, -1) {
		if len(list) == 2 {
			isTimestamp := false
			fieldName := list[1]
			if strings.HasSuffix(fieldName, "#timestamp") {
				fieldName = fieldName[:len(fieldName)-len("#timestamp")]
				isTimestamp = true
			}

			ctx.parsedIndex = append(ctx.parsedIndex, indexField{
				Replace:     list[0],
				FieldName:   fieldName,
				IsTimestamp: isTimestamp,
			})
		}
	}

	// Получаем максимальный размер пачки данных для отправки в эластик.
	ctx.bulkSize, err = strconv.Atoi(output.FLBPluginConfigKey(plugin, "bulksize"))
	if err != nil {
		fmt.Println(err)
		return output.FLB_ERROR
	}
	if ctx.bulkSize <= 0 {
		ctx.bulkSize = 5000
	}

	// Получаем таймаут обработки запроса эластиком.
	ctx.timeout, err = strconv.Atoi(output.FLBPluginConfigKey(plugin, "timeout"))
	if err != nil {
		fmt.Println(err)
		return output.FLB_ERROR
	}
	if ctx.timeout <= 0 {
		ctx.timeout = 5
	}

	// Получаем значение отладочного режима.
	ctx.debug, err = strconv.Atoi(output.FLBPluginConfigKey(plugin, "debug"))
	if err != nil {
		fmt.Println(err)
		return output.FLB_ERROR
	}
	if ctx.debug < 0 || ctx.debug > 2 {
		ctx.debug = 0
	}

	// Сохраняем контекст внутри плагина.
	id := strconv.Itoa(int(rnd.Int63n(1000)))
	contexts[id] = &ctx

	// Отправляем во fluentbit ID контекста.
	output.FLBPluginSetContext(plugin, id)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(plugin, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Gets called with a batch of records to be written to an instance.

	// Получаем контекст.
	id := output.FLBPluginGetContext(plugin).(string)
	ctx := contexts[id]

	indexes := make(map[string][]record)
	dec := output.NewDecoder(data, int(length))

	// Обрабатываем записи от fluentbit.
	for {
		ret, ts, rec := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Частично декодируем запись для удобной дальнейшей обработки.
		m, err := decodeRecord(rec)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Формируем имя индекса.
		index, err := getIndex(m, ts, ctx)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Помещаем запись в список конкретного индекса.
		indexes[index] = append(indexes[index], m)
	}

	wg := &sync.WaitGroup{}

	// Отправляем данные в эластик.
	for index, list := range indexes {
		if len(list) == 0 {
			continue
		}

		// Параллельно отправляем данные пачками.
		for i := 0; i < len(list); i += ctx.bulkSize {
			data := list[i:]
			if len(data) > ctx.bulkSize {
				data = data[:ctx.bulkSize]
			}

			wg.Add(1)
			go flushDataToElastic(index, data, ctx, wg)
		}

		// Ждем завершения всех запрос по текущему индексу.
		wg.Wait()
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

// decodeRecord частично декодирует запись для удобной работы с ней.
func decodeRecord(record map[interface{}]interface{}) (record, error) {
	var err error
	m := make(map[string]interface{})

	for k, v := range record {
		kk, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("bad key type: %T", k)
		}

		switch x := v.(type) {
		case []uint8:
			m[kk] = string(x)
		case map[interface{}]interface{}:
			m[kk], err = decodeRecord(x)
			if err != nil {
				return nil, err
			}
		default:
			m[kk] = x
		}
	}

	return m, nil
}

// getIndex создает имя индекса, в который нужно поместить запись.
func getIndex(m map[string]interface{}, ts interface{}, ctx *Ctx) (string, error) {
	indexName := ctx.index

	// Обходим все параметры шаблона.
	for _, field := range ctx.parsedIndex {
		// Игнорируем отсутствующее значение и несоответствующий тип.
		value, _ := m[field.FieldName].(string)
		if value == "" {
			value = field.FieldName
		}

		// Если поле является отметкой времени события,
		// вставляем год,месяц,день в формате YYYY.MM.DD
		if field.IsTimestamp {
			if t, err := time.Parse(time.RFC3339, value); err == nil {
				// Если смогли разобрать время события, используем его.
				value = fmt.Sprintf("%04d.%02d.%02d", t.Year(), t.Month(), t.Day())
			} else {
				// В противном случае, пытаемся получить время от fluentbit.
				var timestamp time.Time
				switch t := ts.(type) {
				case output.FLBTime:
					timestamp = ts.(output.FLBTime).Time
				case uint64:
					timestamp = time.Unix(int64(t), 0)
				default:
					// В крайнем случае берем текущее время на сервере.
					fmt.Println("time provided invalid, defaulting to now.")
					timestamp = time.Now()
				}

				value = fmt.Sprintf("%d.%d.%d", timestamp.Year(), timestamp.Month(), timestamp.Day())
			}
		}

		// Заменяем параметр его значением.
		indexName = strings.Replace(indexName, field.Replace, value, 1)
	}

	// Эластик принимает имя индекса только в нижнем регистре.
	return strings.ToLower(indexName), nil
}

// Синхронно отправляет одну пачку данных в эластик.
func flushDataToElastic(index string, list []record, ctx *Ctx, wg *sync.WaitGroup) {
	defer wg.Done()

	// Имя индекса задаем в url запроса.
	s := []byte("{ \"index\":{} }\n")

	var buf bytes.Buffer
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)

	// Готовим тело запроса.
	for _, item := range list {
		b.Reset()
		err := enc.Encode(item)

		if err != nil {
			fmt.Println(err)
			continue
		}

		buf.Write(s)
		buf.Write(b.Bytes())
		buf.WriteByte('\n')
	}

	if buf.Len() == 0 {
		return
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod("POST")
	req.SetRequestURI(ctx.url + index + "/_bulk")
	req.Header.SetContentType("application/json")
	req.SetBody(buf.Bytes())

	// Выводим отладочную информацию.
	if ctx.debug > 0 {
		fmt.Println(string(req.RequestURI()))
	}
	if ctx.debug > 1 {
		fmt.Println(string(req.Body()))
	}

	// Отправляем данные в эластик.
	if err := fasthttp.DoTimeout(req, resp, time.Duration(ctx.timeout)*time.Second); err != nil {
		fmt.Println(err)
	}
}
