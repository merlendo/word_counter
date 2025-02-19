FROM debian:bullseye

# Installer les dépendances.
RUN apt-get update && apt-get install -y \
    python3 python3-venv python3-pip \
    openjdk-11-jdk \
    curl \
    procps \
    build-essential \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME et ajouter Rust au PATH.
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64"  
ENV PATH="/root/.cargo/bin:${PATH}"

# Définir le répertoire de travail.
WORKDIR /word_counter
COPY . /word_counter

# Créer les environnement.
RUN python3 -m venv /word_counter/benchmark-env  
ENV PATH="/word_counter/benchmark-env/bin:$PATH"  
ENV VIRTUAL_ENV="/word_counter/benchmark-env"

# Installer les dépendances python.
RUN pip install --no-cache-dir maturin pyspark regex

# Compiler et installer les paquets via maturin.
RUN maturin develop --release

# Démarrer un shell interactif.
CMD [ "/bin/bash" ]
