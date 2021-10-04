package service

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type Service struct {
	Name   string                 // 映射结构体名称
	Typ    reflect.Type           // 结构体类型
	Rcvr   reflect.Value          // 结构体的实例本身
	Method map[string]*MethodType // 该结构体的所有方法
}

func NewService(rcvr interface{}) *Service {
	s := new(Service)
	s.Rcvr = reflect.ValueOf(rcvr)                  // 创建实例
	s.Name = reflect.Indirect(s.Rcvr).Type().Name() // 对象名称，方便调用
	s.Typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.Name) {
		log.Fatalf("rpc server: %s is not a vaild Service Name", s.Name)
	}
	s.registerMethods()
	return s
}

func (s *Service) registerMethods() {
	s.Method = make(map[string]*MethodType)
	for i := 0; i < s.Typ.NumMethod(); i++ {
		method := s.Typ.Method(i)
		mType := method.Type
		// 过滤符合条件的方法，因为定义的入参只有三个，一个是自己本身，还有两个分别对应参数和返回值； 出参应当只有error
		// 1、两个导出或内置类型的入参
		// 2、返回值有且只有一个，类型为error
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		// 判断返回值得类型是否是error类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.Method[method.Name] = &MethodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.Name, method.Name)

	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

func (s *Service) Call(m *MethodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.Rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
