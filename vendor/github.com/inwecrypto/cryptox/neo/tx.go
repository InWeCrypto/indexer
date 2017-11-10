package neo

import (
	"bytes"
	"crypto/elliptic"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/btcsuite/btcutil/base58"
)

func calcTxInput(amount float64, input []*UTXO) (totalAmount float64, payload []byte, err error) {

	sort.Sort(utxoByValue(input))

	inputAmount := float64(0)

	for _, utxo := range input {
		val, err := utxo.Value()

		if err != nil {
			return 0, nil, fmt.Errorf("get utxo value error : %s", err)
		}

		inputAmount += val
	}

	if inputAmount < amount {
		return 0, nil, fmt.Errorf("insufficent input utxo: got %f expect %f", inputAmount, amount)
	}

	var output []*UTXO

	for i := 0; totalAmount < amount; i++ {
		val, _ := input[i].Value()

		totalAmount += val

		output = append(output, input[i])
	}

	var buff bytes.Buffer

	buff.WriteByte(byte(len(output)))

	for _, utxo := range output {
		txid, err := utxo.TxHex()

		if err != nil {
			return 0, nil, fmt.Errorf("get utxo txid[%s] failed :%s", utxo.TransactionID, err)
		}

		buff.Write(reverseBytes(txid))

		binaryN := make([]byte, 2)

		binary.LittleEndian.PutUint16(binaryN, uint16(utxo.Vout.N))

		buff.Write(binaryN)
	}

	payload = buff.Bytes()

	return
}

func packRawTx(asset string, payload []byte, amount, totalAmount float64, from, to string, attrs []*TxAttr) ([]byte, error) {

	var payloadOfAttrs bytes.Buffer

	for _, attr := range attrs {
		payloadOfAttrs.Write(attr.Bytes())
	}

	rawTx := bytes.NewBuffer([]byte{0x80, 0x00, byte(len(attrs))})

	rawTx.Write(payloadOfAttrs.Bytes())
	rawTx.Write(payload)

	assethex, err := hex.DecodeString(asset)

	if err != nil {
		return nil, err
	}

	if totalAmount > amount {

		rawTx.WriteByte(0x02)

		rawTx.Write(reverseBytes(assethex))

		bytesOfAmount := make([]byte, 8)

		binary.LittleEndian.PutUint64(bytesOfAmount, uint64(amount*100000000))

		rawTx.Write(bytesOfAmount)

		bytesOfToAddr, err := hashAddress(to)

		if err != nil {
			return nil, err
		}

		rawTx.Write(bytesOfToAddr)

		rawTx.Write(reverseBytes(assethex))

		binary.LittleEndian.PutUint64(bytesOfAmount, uint64(totalAmount*100000000)-uint64(amount*100000000))

		rawTx.Write(bytesOfAmount)

		bytesOfToAddr, err = hashAddress(from)

		if err != nil {
			return nil, err
		}

		rawTx.Write(bytesOfToAddr)
	} else {
		rawTx.WriteByte(0x01)

		rawTx.Write(reverseBytes(assethex))

		bytesOfAmount := make([]byte, 8)

		binary.LittleEndian.PutUint64(bytesOfAmount, uint64(amount*100000000))

		rawTx.Write(bytesOfAmount)

		bytesOfToAddr, err := hashAddress(to)

		if err != nil {
			return nil, err
		}

		rawTx.Write(bytesOfToAddr)
	}

	return rawTx.Bytes(), nil
}

func signRawTx(key *Key, rawTx []byte) ([]byte, error) {
	signData, err := key.PrivateKey.Sign(rawTx, elliptic.P256())

	if err != nil {
		return nil, err
	}

	var buff bytes.Buffer

	buff.Write(rawTx)

	buff.Write([]byte{0x01, 0x41, 0x40})

	buff.Write(signData)

	buff.Write([]byte{0x23, 0x21})

	buff.Write(key.PrivateKey.PublicKey.ToBytes())

	buff.WriteByte(0xac)

	return buff.Bytes(), nil
}

func hashAddress(address string) ([]byte, error) {
	result, _, err := base58.CheckDecode(address)

	if err != nil {
		return nil, err
	}

	return result[1:20], nil
}

func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}

	return s
}
