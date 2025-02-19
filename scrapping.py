import time
import csv 
from pathlib import Path
from urllib.request import urlopen

def download_gutenberg_book(ebook_id: str) -> bytes:
    """Télécharge un livre du projet Gutenberg à partir de son ID."""
    try:
        url = f"https://www.gutenberg.org/cache/epub/{ebook_id}/pg{ebook_id}.txt"
        response = urlopen(url)
        
        if response.status == 200:
            return response.read()
        else:
            print(f"\n[{response.status}] Impossible de télécharger le livre {ebook_id}")
            return None
            
    except Exception as e:
        print(f"\nErreur : {e}")
        return None


def create_files(mbs: tuple[int]) -> Path:
    """Generateur qui créer les fichier vide."""
    path_dir = Path("data")
    path_dir.mkdir(exist_ok=True)
    for size in mbs:
        path_file = path_dir / f"{size}MB.txt"
        path_file.touch(exist_ok=True)
        yield path_file


def write_file(file_path: Path, text: bytes):
    """Ecrit le contenu dans le fichier."""
    with open(file_path, "w") as file:
        file.write(text.decode())


def preparation_donnees(ids: list[int],
                        sizes: tuple[int] = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)):
    """Prépare des fichiers textes de plusieurs tailles."""
    MB = (1024*1024)
    mbs = iter(sizes)
    paths = create_files(mbs=sizes)
    text = b""
    current_size = next(mbs)
    current_path = next(paths)
    len_max_id = len(max(ids))
    len_ids = len(ids)
    for i, id in enumerate(ids, start=1):
        print(f"\r{i:{len_max_id}}/{len_ids} livres téléchargés. Taille actuelle des données {len(text)/MB:.2f}MB.", end="")
        bytes_text = download_gutenberg_book(id)
        if bytes_text:
            text += bytes_text
        if len(text) / MB  >= current_size:
            write_file(current_path, text)
            try:
                current_path = next(paths)
                current_size = next(mbs)
            except StopIteration:
                break
    print("\n", end="")


def main():
    start = time.time()
    with open("gutenberg_metadata.csv") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        ids = [row[2].split('/')[-1] for row in reader]
    preparation_donnees(ids)
    # Envion 15min pour 512MB de données à 370mbps vitesse internet (https://fast.com/).
    print(f"Temps d'exécution : {time.time()-start:.2f}s") 


if __name__ == "__main__":
    main()



