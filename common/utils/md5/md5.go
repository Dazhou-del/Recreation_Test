package md5

import (
	"crypto/md5"
	"encoding/hex"
	"golang.org/x/crypto/bcrypt"
)

// BcryptHash 使用 bcrypt 对密码进行加密
func BcryptHash(password string) string {
	bytes, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(bytes)
}

// BcryptCheck 对比明文密码和数据库的哈希值
func BcryptCheck(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func MD5V(str []byte, b ...byte) string {
	h := md5.New()
	h.Write(str)
	return hex.EncodeToString(h.Sum(b))
}

// GetMD5Hash 计算并返回输入字符串的 MD5 哈希值
func GetMD5Hash(password string) string {
	hash := md5.New()
	hash.Write([]byte(password))
	md5Bytes := hash.Sum(nil)

	return hex.EncodeToString(md5Bytes)
}

// CheckPasswordHash 校验输入的密码是否与存储的 MD5 哈希值匹配
func CheckPasswordHash(password, hash string) bool {
	// 计算输入密码的 MD5 哈希值
	passwordHash := GetMD5Hash(password)

	// 比较计算出的哈希值与存储的哈希值
	return passwordHash == hash
}
