package http_api

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

// 请求参数对象
type ReqParams struct {
	url.Values        // 请求的消息头 map[string][]string
	Body       []byte // 请求的消息内容
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	// 从请求中解析出参数，返回类型为 map[string][]string
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	// 返回请求的消息体
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

// 根据key获取value（第一个），因为消息头都是键值对
func (r *ReqParams) Get(key string) (string, error) {
	v, ok := r.Values[key]
	if !ok {
		return "", errors.New("key not in query params")
	}
	return v[0], nil
}

// 根据key获取value（全部），因为消息头都是键值对
func (r *ReqParams) GetAll(key string) ([]string, error) {
	v, ok := r.Values[key]
	if !ok {
		return nil, errors.New("key not in query params")
	}
	return v, nil
}

// 出来Post方法的参数
type PostParams struct {
	*http.Request
}

func (p *PostParams) Get(key string) (string, error) {
	if p.Request.Form == nil {
		p.Request.ParseMultipartForm(1 << 20)
	}

	if vs, ok := p.Request.Form[key]; ok {
		return vs[0], nil
	}
	return "", errors.New("key not in post params")
}
