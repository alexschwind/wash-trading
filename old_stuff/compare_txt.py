def compare_txt_files(file1, file2):
    differences = 0
    with open(file1, "r") as f1, open(file2, "r") as f2:
        line_num = 1
        while True:
            line1 = f1.readline().strip()
            line2 = f2.readline().strip()

            if line1 != line2:
                print(f"❌ Line {line_num} differs:")
                print(f"   File 1: {line1}")
                print(f"   File 2: {line2}")
                break
            line_num += 1
            if line1 == "" or line2 == "":
                break

    if differences == 0:
        print("✅ Files match perfectly.")
    else:
        print(f"🔎 Total differing lines: {differences}")

    

# Example usage:
compare_txt_files("test_py.txt", "test_r2.txt")