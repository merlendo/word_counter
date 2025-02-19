import time
from collections import Counter 
from pathlib import Path
from urllib.request import urlopen

from pyspark.sql import SparkSession
import regex as re # Important pour que l'engine regex soit le même que celui de rust.

import word_counter # Module rust pour compter les mots.


def print_result_benchmark(results: dict) -> None:
    """Affiche les résultats sous forme d'un tableau."""
    # Afficher l'en-tête.
    cols = ["Taille (MB)", "Python Time (s)", "Spark Time (s)", "Python optimized Time (s)", "Rust Time (s)", "Speedup", "Mots uniques"]
    header = " | ".join(cols) 
    header += " |"
    print(header)
    print("-" * len(header))
    
    # Afficher les lignes de résultats.
    for r in results:
        for i, v in enumerate(r.values()):
            taille_col = len(cols[i]) 
            taille_v = len(str(v))
            padding = taille_col
            print(f"{round(v, 2):>{padding}} | ", end="")
        print("\n", end="")


def get_stopword(file_path: str) -> set:
    """Récupère un set de stopwords."""
    with open(file_path) as file:
        return set([line.strip() for line in file]) 


def wordcount_python(file_path: Path) -> dict:
    """Effectue le wordcount avec Python."""
    # Liste de stopwords.
    stop_words = get_stopword("english")
   
    # Liste de mots.
    word_list = []
    regex = re.compile(r'[^\w\s]')
    with open(file_path, "r", encoding='utf-8') as file:
        for line in file:
            texte = regex.sub(' ', line.lower())
            word_list.extend([word for word in texte.split() if word not in stop_words])
    
    # Comptage.
    result = dict(Counter(word_list))

    return result


def wordcount_python_opti(file_path: Path) -> dict:
    """Credit to https://benhoyt.com/writings/count-words/"""
    
    # Stop words.
    stop_words = get_stopword("english")
    
    # Counter.
    counts = Counter()
    
    remaining = ''
    regex = re.compile(r'[^\w\s]')
    #regex = re.compile(r"[^\p{L}\s]")
    file = open(file_path, "r")
    while True:
        chunk = remaining + file.read(64 * 1024)
        if not chunk:
            file.close()
            break
        
        last_lf = chunk.rfind('\n')  # Process to last LF character
        if last_lf == -1:
            remaining = ''
        else:
            remaining = chunk[last_lf + 1:]
            chunk = chunk[:last_lf]
    
        chunk = regex.sub(" ", chunk.lower())
        words = [word for word in chunk.split() if word not in stop_words]
        counts.update(words)
    
    return dict(counts)


def wordcount_spark(spark: SparkSession, file_path: str) -> dict:
    """Effectue le wordcount avec Spark."""
    
    # Lecture et transformation du texte en RDD. 
    texte = spark.sparkContext.textFile(file_path)
    words = texte.flatMap(lambda line: re.sub(r'[^\w\s]', ' ', line.lower()).split())
    
    # Filtrage des stopwords.
    stop_words = get_stopword("english")
    words = words.filter(lambda word: word not in stop_words)
    
    # Comptage.
    mapped = words.map(lambda x: (x, 1))
    reduced = mapped.reduceByKey(lambda x, y: x + y)
    result = dict(reduced.collect())
    
    return result


def run_benchmark() -> list[dict]:
    """Exécute le benchmark complet"""
    # Initialisation d'une session spark.
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "4") \
        .getOrCreate()
    
    results = []
    
    # Tris les chemins de fichier par tailles.
    paths = [p for p in Path("data").iterdir() if p.stem.replace("MB", "").isdigit()]
    paths = sorted(paths, key=lambda p: int(p.stem.replace("MB", "")))
    
    for file_path in paths:

        size = int(file_path.stem.replace("MB", ""))
        print(f"\nTest avec fichier {file_path.name}...")
        
        # Test Python.
        print("Exécution version Python...")
        start = time.time()
        python_counts = wordcount_python(file_path)
        python_time = time.time() - start
        
        # Test Spark.
        print("Exécution version Spark...")
        start = time.time()
        spark_counts = wordcount_spark(spark, str(file_path))
        spark_time = time.time() - start

        # Test Python optimisé.
        print("Exécution version Python optimisé...")
        start = time.time()
        python_counts_opti = wordcount_python_opti(file_path)
        python_time_opti = time.time() - start

        # Test Python optimisé.
        print("Exécution version Rust...")
        start = time.time()
        rust_counts = word_counter.count_words_rust(str(file_path), "english")
        rust_time = time.time() - start

        results.append({
            'size_mb': size,
            'python_time': python_time,
            'spark_time': spark_time,
            'python_time_opti': python_time_opti,
            'rust_time': rust_time,
            'speedup': python_time / spark_time if spark_time > 0 else float('inf'),
            'unique_words': len(python_counts)
        })
        
        print(f"Python time: {python_time:.2f}s")
        print(f"Spark time: {spark_time:.2f}s")
        print(f"Python optimized time: {python_time_opti:.2f}s")
        print(f"Rust time: {rust_time:.2f}s")
        print(f"Speedup: {python_time/spark_time:.2f}x")
        print(f"Nombre de mots uniques spark: {len(spark_counts)}")
        print(f"Nombre de mots uniques python: {len(python_counts)}")
        print(f"Nombre de mots uniques python optimisé: {len(python_counts_opti)}")
        print(f"Nombre de mots uniques rust: {len(rust_counts)}")
    
    spark.stop()
    return results


def main():
    # Exécution du benchmark.
    print("\nExécution du benchmark...")
    results = run_benchmark()
    
    # Affichage des résultats.
    print("\nRésultats du benchmark:")
    print_result_benchmark(results)
    return results


if __name__ == "__main__":
    main()


