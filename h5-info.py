import argparse
import h5py

def list_keys(h5_file_path):
    try:
        with h5py.File(h5_file_path, 'r') as f:
            print(f"'{h5_file_path}' içindeki yapılar:")
            def print_structure(name):
                print(f" - {name}")
            f.visit(print_structure)
    except Exception as e:
        print(f"Hata oluştu: {e}")

def main():
    parser = argparse.ArgumentParser(description='HDF5 (.h5) dosyasındaki key/yapıları listele')
    parser.add_argument('file', type=str, help='HDF5 dosyasının yolu (.h5 uzantılı)')
    args = parser.parse_args()

    list_keys(args.file)

if __name__ == '__main__':
    main()
