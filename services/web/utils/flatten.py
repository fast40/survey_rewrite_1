import argparse
import pathlib


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('directory', help='')

    args = parser.parse_args()

    root = pathlib.Path(args.directory)

    for group in root.glob('*'):
        for batch in group.glob('*'):
            batch.rename(root / (batch.parent.name + '_' + batch.name))

        group.rmdir()


if __name__ == '__main__':
    main()
