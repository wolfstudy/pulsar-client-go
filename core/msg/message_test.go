package msg

import(
	"testing"
)

func TestDecodeBatchPayload(t *testing.T){
	payload := []byte{0,0,0,2,24,12,104,101,108,108,111,45,112,117,108,115,97,114} // hello-pulsar
	list,err := DecodeBatchPayload(payload,1)
	if err!=nil{
		t.Fatal(err)
	}
	if get,want := len(list),1;get!=want{
		t.Errorf("want %v, but get %v",get,want)
	}
	
	m := list[0]
	if get,want := string(m.SinglePayload), "hello-pulsar";get!=want{
		t.Errorf("want %v, but get %v",get,want)
	}
}