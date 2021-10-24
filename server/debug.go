package server

import (
	"fmt"
	"grpc_lee/service"
	"html/template"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHTTP struct {
	*Server
}

type debugService struct {
	Name   string
	Method map[string]*service.MethodType
}

// 用于外部查看服务调用情况
func (d debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var services []debugService
	d.serviceMap.Range(func(namei, svici interface{}) bool {
		svc := svici.(*service.Service)
		services = append(services, debugService{
			Name:   namei.(string),
			Method: svc.Method,
		})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
