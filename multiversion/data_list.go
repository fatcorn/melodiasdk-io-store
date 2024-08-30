package multiversion

import (
	"cosmossdk.io/store/types"
	"sync"
)

const (
	multiVersionTotalIncarnation = 20
)

var _ MultiVersionValue = (*multiVersionListItem)(nil)

func NewMultiVersionListItem(size int) *multiVersionListItem {
	valueList := make([]MultiVersionValueItem, size)
	return &multiVersionListItem{
		valueList: valueList,
	}
}

type multiVersionListItem struct {
	valueList []MultiVersionValueItem // contains versions values written to this key
	mtx       sync.RWMutex            // manages read + write accesses
}

func (m multiVersionListItem) GetLatest() (value MultiVersionValueItem, found bool) {
	for i := range m.valueList {
		incarnationValue := m.valueList[len(m.valueList)-i-1]
		if incarnationValue != nil {
			return incarnationValue, true
		}
	}
	return nil, false
}

func (m multiVersionListItem) GetLatestNonEstimate() (value MultiVersionValueItem, found bool) {
	for i := range m.valueList {
		incarnationValue := m.valueList[len(m.valueList)-i-1]
		if incarnationValue != nil && !incarnationValue.IsEstimate() {
			return incarnationValue, true
		}
	}
	return nil, false
}

func (m multiVersionListItem) GetLatestBeforeIndex(index int) (value MultiVersionValueItem, found bool) {

	if index >= len(m.valueList) {
		return nil, false
	}
	for i := index - 1; i >= 0; i-- {
		incarnationValue := m.valueList[i]
		if incarnationValue != nil {
			return incarnationValue, true
		}
	}
	return nil, false
}

func (m multiVersionListItem) Set(index int, incarnation int, value []byte) {
	types.AssertValidValue(value)
	valueItem := NewValueItem(index, incarnation, value)
	m.setCommonValue(index, incarnation, valueItem)
}

func (m multiVersionListItem) SetEstimate(index int, incarnation int) {
	estimateItem := NewEstimateItem(index, incarnation)
	m.setCommonValue(index, incarnation, estimateItem)
}

func (m multiVersionListItem) setCommonValue(index int, incarnation int, valueItem MultiVersionValueItem) {
	m.valueList[index] = valueItem

}

func (m multiVersionListItem) Delete(index int, incarnation int) {
	deletedItem := NewDeletedItem(index, incarnation)
	m.setCommonValue(index, incarnation, deletedItem)
}

func (m multiVersionListItem) Remove(index int) {
	m.valueList[index] = nil
}
