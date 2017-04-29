package http_api

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// 带有超时作用的连接对象
type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

// 带有超时的io.Reader接口实现
func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

// 带有超时的io.Writer接口实现
func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

// A custom http.Transport with support for deadline timeouts
func NewDeadlineTransport(timeout time.Duration) *http.Transport {
	// 带有超时机制的HTTP传输对象创建
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

// HTTP请求的客户端对象
type Client struct {
	c *http.Client
}

// 创建HTTP客户端对象
func NewClient(tlsConfig *tls.Config) *Client {
	transport := NewDeadlineTransport(2 * time.Second)
	transport.TLSClientConfig = tlsConfig
	return &Client{
		c: &http.Client{
			Transport: transport,
		},
	}
}

// NegotiateV1 is a helper function to perform a v1 HTTP request
// and fallback to parsing the old backwards-compatible response format
// storing the result in the value pointed to by v.
//
// TODO: deprecated, remove in 1.0 (replace calls with GETV1)
func (c *Client) NegotiateV1(endpoint string, v interface{}) error {
retry:
	// 创建请求对象
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	// 发送请求并且等待回复
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	// 读取回复内容
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	// 如果不是200就继续请求
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			// 如果状态值为403,同时url不带https，那么创建https请求
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	// 没有消息体
	if len(body) == 0 {
		body = []byte("{}")
	}

	// unwrap pre-1.0 api response
	if resp.Header.Get("X-NSQ-Content-Type") != "nsq; version=1.0" {
		var u struct {
			StatusCode int64           `json:"status_code"`
			Data       json.RawMessage `json:"data"`
		}
		err := json.Unmarshal(body, u)
		if err != nil {
			return err
		}
		if u.StatusCode != 200 {
			return fmt.Errorf("got 200 response, but api status code of %d", u.StatusCode)
		}
		body = u.Data
	}

	return json.Unmarshal(body, v) // 调用者通过v获取到内容
}

// GETV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) GETV1(endpoint string, v interface{}) error {
retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

// PostV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) POSTV1(endpoint string) error {
retry:
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	return nil
}

func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}

	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}
