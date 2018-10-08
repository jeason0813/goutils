package def

type Exception interface {
	error
	Info() interface{}
	Log(context Context)
	No() int
}

type exception struct {
	info   interface{}
	detail interface{}
	show   bool
	no     int
	t      string
	log    bool
}

//  不显示错误：显示错误类型
//  显示错误：
// 		字符串错误：显示字符串
// 		结构类型错误：显示错误类型（info中显示错误）
func (e *exception) Error() string {
	s, ok := e.info.(string)
	if e.show && ok {
		return s
	} else {
		return e.t
	}
}

//  不显示错误：返回空
//  显示错误：
//		字符串错误：显示详细错误
// 		结构类型错误：显示错误（详细错误，显示在log中）
func (e *exception) Info() interface{} {
	if !e.show {
		return nil
	}
	_, ok := e.info.(string)
	if !ok {
		return e.info
	} else {
		return e.detail
	}
}

func (e *exception) No() int {
	return e.no
}

func (e *exception) Log(context Context) {
	if e.log {
		context.Logger().Error(e.info, e.detail)
	}
}

func GenerateException(no int, t string, show bool, log bool) func(message interface{}) Exception {
	return func(info interface{}) Exception {
		return &exception{
			info,
			nil,
			show,
			no,
			t,
			log,
		}
	}
}
func GenerateExceptionDetail(no int, t string, show bool, log bool) func(message interface{}, detail interface{}) Exception {
	return func(info interface{}, detail interface{}) Exception {
		return &exception{
			info,
			detail,
			show,
			no,
			t,
			log,
		}
	}
}