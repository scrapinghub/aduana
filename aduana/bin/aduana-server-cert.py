from OpenSSL import crypto


def generate_certificate(prefix='aduana-server', name='Aduana Server'):
    key_path = prefix + '.key'
    cert_path = prefix + '.crt'

    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 2048)
    with open(key_path, 'w') as key_file:
        key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))
    print 'Generated key file at', key_path

    cert = crypto.X509()
    cert.set_version(2)
    cert.set_serial_number(1)
    cert.get_subject().CN = name
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(24*60*60)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(key)
    cert.sign(key, 'sha1')

    with open(cert_path, 'w') as cert_file:
        cert_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    print 'Generated certificate file at', cert_path

if __name__ == '__main__':
    generate_certificate()
