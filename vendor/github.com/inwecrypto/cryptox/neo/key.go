package neo

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"

	"github.com/inwecrypto/cryptox/btc"
	"github.com/inwecrypto/cryptox/keystore"
	"github.com/pborman/uuid"
	"golang.org/x/crypto/ripemd160"
)

// const variables
var (
	StandardScryptN = 1 << 18
	StandardScryptP = 1
	LightScryptN    = 1 << 12
	LightScryptP    = 6
)

var secp256r1 btc.EllipticCurve

func init() {
	/* See Certicom's SEC2 2.7.1, pg.15 */
	/* secp256k1 elliptic curve parameters */
	// secp256k1.P, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16)
	// secp256k1.A, _ = new(big.Int).SetString("0000000000000000000000000000000000000000000000000000000000000000", 16)
	// secp256k1.B, _ = new(big.Int).SetString("0000000000000000000000000000000000000000000000000000000000000007", 16)
	// secp256k1.G.X, _ = new(big.Int).SetString("79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798", 16)
	// secp256k1.G.Y, _ = new(big.Int).SetString("483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8", 16)
	// secp256k1.N, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16)
	// secp256k1.H, _ = new(big.Int).SetString("01", 16)

	secp256r1.P, _ = new(big.Int).SetString("FFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF", 16) //Q
	secp256r1.A, _ = new(big.Int).SetString("FFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFC", 16)
	secp256r1.B, _ = new(big.Int).SetString("5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B", 16)
	secp256r1.G.X, _ = new(big.Int).SetString("6B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296", 16)
	secp256r1.G.Y, _ = new(big.Int).SetString("4FE342E2FE1A7F9B8EE7EB4A7C0F9E162BCE33576B315ECECBB6406837BF51F5", 16)
	secp256r1.N, _ = new(big.Int).SetString("FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551", 16)
	secp256r1.H, _ = new(big.Int).SetString("01", 16)
}

// Key NEO wallet key
type Key struct {
	ID         uuid.UUID       // Key ID
	Address    string          // address
	PrivateKey *btc.PrivateKey // btc private key
}

// NewKey create new key
func NewKey() (*Key, error) {
	privateKey, err := btc.GenerateKey(secp256r1, rand.Reader)

	if err != nil {
		return nil, err
	}

	return &Key{
		ID:         uuid.NewRandom(),
		PrivateKey: &privateKey,
		Address:    toNeoAddress(&privateKey.PublicKey),
	}, nil
}

// KeyFromPrivateKey neo key from private key bytes
func KeyFromPrivateKey(privateKeyBytes []byte) (*Key, error) {
	privateKey := new(btc.PrivateKey)

	err := privateKey.FromBytes(privateKeyBytes, secp256r1)

	if err != nil {
		return nil, err
	}

	return &Key{
		ID:         uuid.NewRandom(),
		PrivateKey: privateKey,
		Address:    toNeoAddress(&privateKey.PublicKey),
	}, nil
}

// KeyFromWIF neo key from wif format
func KeyFromWIF(wif string) (*Key, error) {
	privateKey := new(btc.PrivateKey)

	err := privateKey.FromWIF(wif, secp256r1)

	if err != nil {
		return nil, err
	}

	return &Key{
		ID:         uuid.NewRandom(),
		PrivateKey: privateKey,
		Address:    toNeoAddress(&privateKey.PublicKey),
	}, nil
}

func keystoreKeyToNEOKey(key *keystore.Key) (*Key, error) {

	privateKey := new(btc.PrivateKey)

	err := privateKey.FromBytes(key.PrivateKey, secp256r1)

	if err != nil {
		return nil, err
	}

	return &Key{
		ID:         uuid.UUID(key.ID),
		Address:    key.Address,
		PrivateKey: privateKey,
	}, nil
}

func neoKeyToKeyStoreKey(key *Key) (*keystore.Key, error) {
	bytes := key.PrivateKey.ToBytes()

	return &keystore.Key{
		ID:         key.ID,
		Address:    key.Address,
		PrivateKey: bytes,
	}, nil
}

// WriteScryptKeyStore write keystore with Scrypt format
func WriteScryptKeyStore(key *Key, password string) ([]byte, error) {
	keyStoreKey, err := neoKeyToKeyStoreKey(key)

	if err != nil {
		return nil, err
	}

	attrs := map[string]interface{}{
		"ScryptN": StandardScryptN,
		"ScryptP": StandardScryptP,
	}

	return keystore.Encrypt(keyStoreKey, password, attrs)
}

// WriteLightScryptKeyStore write keystore with Scrypt format
func WriteLightScryptKeyStore(key *Key, password string) ([]byte, error) {
	keyStoreKey, err := neoKeyToKeyStoreKey(key)

	if err != nil {
		return nil, err
	}

	attrs := map[string]interface{}{
		"ScryptN": LightScryptN,
		"ScryptP": LightScryptP,
	}

	return keystore.Encrypt(keyStoreKey, password, attrs)
}

// ReadKeyStore read key from keystore
func ReadKeyStore(data []byte, password string) (*Key, error) {
	keystore, err := keystore.Decrypt(data, password)

	if err != nil {
		return nil, err
	}

	return keystoreKeyToNEOKey(keystore)
}

func toNeoAddress(publickKey *btc.PublicKey) (address string) {
	/* See https://en.bitcoin.it/wiki/Technical_background_of_Bitcoin_addresses */

	/* Convert the public key to bytes */
	pubbytes := publickKey.ToBytes()

	pubbytes = append([]byte{0x21}, pubbytes...)
	pubbytes = append(pubbytes, 0xAC)

	/* SHA256 Hash */
	sha256h := sha256.New()
	sha256h.Reset()
	sha256h.Write(pubbytes)
	pubhash1 := sha256h.Sum(nil)

	/* RIPEMD-160 Hash */
	ripemd160h := ripemd160.New()
	ripemd160h.Reset()
	ripemd160h.Write(pubhash1)
	pubhash2 := ripemd160h.Sum(nil)

	programhash := pubhash2

	//wallet version
	//program_hash = append([]byte{0x17}, program_hash...)

	// doublesha := sha256Bytes(sha256Bytes(program_hash))

	// checksum := doublesha[0:4]

	// result := append(program_hash, checksum...)
	/* Convert hash bytes to base58 check encoded sequence */
	address = b58checkencodeNEO(0x17, programhash)

	return address
}

func b58checkencodeNEO(ver uint8, b []byte) (s string) {
	/* Prepend version */
	bcpy := append([]byte{ver}, b...)

	/* Create a new SHA256 context */
	sha256h := sha256.New()

	/* SHA256 Hash #1 */
	sha256h.Reset()
	sha256h.Write(bcpy)
	hash1 := sha256h.Sum(nil)

	/* SHA256 Hash #2 */
	sha256h.Reset()
	sha256h.Write(hash1)
	hash2 := sha256h.Sum(nil)

	/* Append first four bytes of hash */
	bcpy = append(bcpy, hash2[0:4]...)

	/* Encode base58 string */
	s = b58encode(bcpy)

	// /* For number of leading 0's in bytes, prepend 1 */
	// for _, v := range bcpy {
	// 	if v != 0 {
	// 		break
	// 	}
	// 	s = "1" + s
	// }

	return s
}

func b58encode(b []byte) (s string) {
	/* See https://en.bitcoin.it/wiki/Base58Check_encoding */

	const BitcoinBase58Table = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

	/* Convert big endian bytes to big int */
	x := new(big.Int).SetBytes(b)

	/* Initialize */
	r := new(big.Int)
	m := big.NewInt(58)
	zero := big.NewInt(0)
	s = ""

	/* Convert big int to string */
	for x.Cmp(zero) > 0 {
		/* x, r = (x / 58, x % 58) */
		x.QuoRem(x, m, r)
		/* Prepend ASCII character */
		s = string(BitcoinBase58Table[r.Int64()]) + s
	}

	return s
}

// b58decode decodes a base-58 encoded string into a byte slice b.
func b58decode(s string) (b []byte, err error) {
	/* See https://en.bitcoin.it/wiki/Base58Check_encoding */

	const BitcoinBase58Table = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

	/* Initialize */
	x := big.NewInt(0)
	m := big.NewInt(58)

	/* Convert string to big int */
	for i := 0; i < len(s); i++ {
		b58index := strings.IndexByte(BitcoinBase58Table, s[i])
		if b58index == -1 {
			return nil, fmt.Errorf("Invalid base-58 character encountered: '%c', index %d", s[i], i)
		}
		b58value := big.NewInt(int64(b58index))
		x.Mul(x, m)
		x.Add(x, b58value)
	}

	/* Convert big int to big endian bytes */
	b = x.Bytes()

	return b, nil
}

/******************************************************************************/
/* Base-58 Check Encode/Decode */
/******************************************************************************/

// b58checkencode encodes version ver and byte slice b into a base-58 check encoded string.
func b58checkencode(ver uint8, b []byte) (s string) {
	/* Prepend version */
	bcpy := append([]byte{ver}, b...)

	/* Create a new SHA256 context */
	sha256h := sha256.New()

	/* SHA256 Hash #1 */
	sha256h.Reset()
	sha256h.Write(bcpy)
	hash1 := sha256h.Sum(nil)

	/* SHA256 Hash #2 */
	sha256h.Reset()
	sha256h.Write(hash1)
	hash2 := sha256h.Sum(nil)

	/* Append first four bytes of hash */
	bcpy = append(bcpy, hash2[0:4]...)

	/* Encode base58 string */
	s = b58encode(bcpy)

	/* For number of leading 0's in bytes, prepend 1 */
	for _, v := range bcpy {
		if v != 0 {
			break
		}
		s = "1" + s
	}

	return s
}

// b58checkdecode decodes base-58 check encoded string s into a version ver and byte slice b.
func b58checkdecode(s string) (ver uint8, b []byte, err error) {
	/* Decode base58 string */
	b, err = b58decode(s)
	if err != nil {
		return 0, nil, err
	}

	/* Add leading zero bytes */
	for i := 0; i < len(s); i++ {
		if s[i] != '1' {
			break
		}
		b = append([]byte{0x00}, b...)
	}

	/* Verify checksum */
	if len(b) < 5 {
		return 0, nil, fmt.Errorf("Invalid base-58 check string: missing checksum")
	}

	/* Create a new SHA256 context */
	sha256h := sha256.New()

	/* SHA256 Hash #1 */
	sha256h.Reset()
	sha256h.Write(b[:len(b)-4])
	hash1 := sha256h.Sum(nil)

	/* SHA256 Hash #2 */
	sha256h.Reset()
	sha256h.Write(hash1)
	hash2 := sha256h.Sum(nil)

	/* Compare checksum */
	if bytes.Compare(hash2[0:4], b[len(b)-4:]) != 0 {
		return 0, nil, fmt.Errorf("invalid base-58 check string: invalid checksum")
	}

	/* Strip checksum bytes */
	b = b[:len(b)-4]

	/* Extract and strip version */
	ver = b[0]
	b = b[1:]

	return ver, b, nil
}
