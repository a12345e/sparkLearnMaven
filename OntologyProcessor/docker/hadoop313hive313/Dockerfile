# Hadoop HDFS + Apache Hive in one image; the role is chosen at runtime by the
# entrypoint, so namenode, datanode, metastore and hiveserver2 all share it.
#
# Java 8 is deliberate: it is what Hadoop 3.1.3 and Hive 3.1.3 are built and
# tested against, and it matches this project's maven.compiler.target.
FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.1.3
ARG HIVE_VERSION=3.1.3
# Tez 0.10.1 is the release built against Hadoop 3.1.3.
ARG TEZ_VERSION=0.10.1
ARG POSTGRES_JDBC_VERSION=42.7.3
ARG APACHE_ARCHIVE=https://archive.apache.org/dist

ENV HADOOP_VERSION=${HADOOP_VERSION} \
    HIVE_VERSION=${HIVE_VERSION} \
    TEZ_VERSION=${TEZ_VERSION} \
    HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    TEZ_HOME=/opt/tez \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HIVE_CONF_DIR=/opt/hive/conf \
    HADOOP_LOG_DIR=/var/log/hadoop \
    HIVE_LOG_DIR=/var/log/hive \
    HDFS_CONF_DIR=/conf

ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin:${PATH}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl ca-certificates procps netcat-openbsd less; \
    rm -rf /var/lib/apt/lists/*

# --- Hadoop -----------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "${APACHE_ARCHIVE}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -o /tmp/hadoop.tar.gz; \
    curl -fsSL "${APACHE_ARCHIVE}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512" \
        -o /tmp/hadoop.sha512; \
    printf '%s  /tmp/hadoop.tar.gz\n' \
        "$(tr -d '\r' < /tmp/hadoop.sha512 | awk '{print $NF}' | tr 'A-F' 'a-f')" \
        | sha512sum -c -; \
    mkdir -p ${HADOOP_HOME}; \
    tar -xzf /tmp/hadoop.tar.gz -C ${HADOOP_HOME} --strip-components=1; \
    rm -f /tmp/hadoop.tar.gz /tmp/hadoop.sha512; \
    rm -rf ${HADOOP_HOME}/share/doc

# --- Hive -------------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "${APACHE_ARCHIVE}/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
        -o /tmp/hive.tar.gz; \
    mkdir -p ${HIVE_HOME}; \
    tar -xzf /tmp/hive.tar.gz -C ${HIVE_HOME} --strip-components=1; \
    rm -f /tmp/hive.tar.gz

# --- Tez --------------------------------------------------------------------
# Hive ships only tez-api, never the Tez runtime. Without this stage every
# query on the Tez engine dies on a missing org.apache.tez.dag class.
RUN set -eux; \
    curl -fsSL "${APACHE_ARCHIVE}/tez/${TEZ_VERSION}/apache-tez-${TEZ_VERSION}-bin.tar.gz" \
        -o /tmp/tez.tar.gz; \
    mkdir -p ${TEZ_HOME}; \
    tar -xzf /tmp/tez.tar.gz -C ${TEZ_HOME} --strip-components=1; \
    rm -f /tmp/tez.tar.gz; \
    rm -f ${TEZ_HOME}/tez-tests-*.jar ${TEZ_HOME}/tez-examples-*.jar \
          ${TEZ_HOME}/tez-build-tools-*.jar ${TEZ_HOME}/tez-javadoc-tools-*.jar

# Put the Tez runtime on Hive's classpath. Its own jars go in wholesale; its
# third-party dependencies only when neither Hive nor Hadoop already ships that
# artifact, so we never end up with two versions of one library.
RUN set -eux; \
    artifact() { basename "$1" | sed 's/-[0-9][0-9.]*\(-[A-Za-z0-9._]*\)\?\.jar$//'; }; \
    find ${HIVE_HOME}/lib ${HADOOP_HOME}/share/hadoop -name '*.jar' -printf '%f\n' \
        | sed 's/-[0-9][0-9.]*\(-[A-Za-z0-9._]*\)\?\.jar$//' | sort -u > /tmp/present.txt; \
    cp ${TEZ_HOME}/tez-*.jar ${TEZ_HOME}/hadoop-shim-*.jar ${HIVE_HOME}/lib/; \
    for jar in ${TEZ_HOME}/lib/*.jar; do \
        if ! grep -qxF "$(artifact "$jar")" /tmp/present.txt; then \
            echo "adding tez dependency $(basename "$jar")"; \
            cp "$jar" ${HIVE_HOME}/lib/; \
        fi; \
    done; \
    rm -f /tmp/present.txt; \
    test -f "$(ls ${HIVE_HOME}/lib/tez-dag-*.jar)"

# --- PostgreSQL JDBC driver, for the metastore schema -----------------------
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        -o ${HIVE_HOME}/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Hadoop and Hive each ship their own Guava, and two copies on one classpath is
# the classic NoSuchMethodError. Keep the newer of the two in both trees.
RUN set -eux; \
    hadoop_guava="$(ls ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar)"; \
    hive_guava="$(ls ${HIVE_HOME}/lib/guava-*.jar 2>/dev/null || true)"; \
    if [ -n "${hive_guava}" ]; then \
        newer="$(printf '%s\n%s\n' "$(basename "${hadoop_guava}")" "$(basename "${hive_guava}")" | sort -V | tail -1)"; \
        if [ "${newer}" = "$(basename "${hadoop_guava}")" ]; then \
            rm -f ${HIVE_HOME}/lib/guava-*.jar; \
            cp "${hadoop_guava}" ${HIVE_HOME}/lib/; \
        else \
            rm -f ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar; \
            cp "${hive_guava}" ${HADOOP_HOME}/share/hadoop/common/lib/; \
        fi; \
    fi; \
    ls ${HADOOP_HOME}/share/hadoop/common/lib/guava-*.jar ${HIVE_HOME}/lib/guava-*.jar

RUN mkdir -p ${HADOOP_LOG_DIR} ${HIVE_LOG_DIR} /hadoop/dfs/name /hadoop/dfs/data ${HDFS_CONF_DIR}

COPY scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
# Belt and braces alongside .gitattributes: a CRLF checkout on Windows would
# otherwise make this "bad interpreter".
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh && chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["namenode"]
