# Use uma imagem base do OpenJDK 17
FROM openjdk:17-jdk-slim

# Instalar Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o arquivo pom.xml e baixar as dependências Maven
COPY pom.xml ./
RUN mvn dependency:go-offline

# Copiar o código do projeto para o diretório de trabalho
COPY src ./src

# Compilar o projeto
RUN mvn clean package

# Comando para executar a aplicação
CMD ["java", "-jar", "target/kafka-batch-processor-1.0-SNAPSHOT.jar"]
