from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding, hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import os


def generate_key():
	salt = os.urandom(16)
	password = b'myencryptionpassword'
	iterations = 100000  # Number of iterations for PBKDF2
	key_size = 32

	# Derive the key using PBKDF2
	kdf = PBKDF2HMAC(
		algorithm=hashes.SHA256(),
		length=key_size,
		salt=salt,
		iterations=iterations,
		backend=default_backend()
	)
	key = kdf.derive(password)
	return key

def encrypt_password(password, key):
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_password = padder.update(password.encode()) + padder.finalize()
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_password) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode()

def decrypt_password(encrypted_password, key):
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())
    decryptor = cipher.decryptor()

    # Base64 decode the ciphertext
    ciphertext = base64.b64decode(encrypted_password.encode())

    # Decrypt the ciphertext



    decrypted_padded_password = decryptor.update(ciphertext) + decryptor.finalize()

    # Create an unpadder for PKCS7 padding
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()

    # Remove padding from the decrypted password
    decrypted_password = unpadder.update(decrypted_padded_password) + unpadder.finalize()

    return decrypted_password.decode()

#key = b'myencryptionkey123'
key = generate_key()
password = 'mysecretpassword'

encrypted_password = encrypt_password(password, key)
print("Encrypted Password:", encrypted_password)

decrypted_password = decrypt_password(encrypted_password, key)
print("Decrypted Password:", decrypted_password)

