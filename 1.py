import chardet

with open('your_file.txt', 'rb') as file:
    result = chardet.detect(file.read())
print(result)
