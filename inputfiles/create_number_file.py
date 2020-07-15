if __name__ == '__main__':
    with open("./numbers.text", "w") as file:
        text = ""
        for i in range(0, 60000):
            text+= str(i) + "\n"
        file.write(text)