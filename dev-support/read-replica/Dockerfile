# Stage 0: Cache Maven dependencies
FROM registry.access.redhat.com/ubi8/openjdk-17:1.23 AS cache-stage

# Switch to user root (UID 0)
USER 0

# Install necessary packages for building Maven dependencies
RUN microdnf update -y && microdnf install -y \
        maven-1:3.8.5-6.module+el8.10.0+21301+657f54a3.noarch \
        git-2.43.7-1.el8_10.aarch64 \
        hostname-3.20-6.el8.aarch64 \
        diffutils-3.6-6.el8.aarch64 \
    && microdnf clean all

# Copy the entire source code to cache dependencies
COPY ./hbase /opt/hbase-src

WORKDIR /opt/hbase-src

# Download and cache all dependencies
RUN mvn clean install -DskipTests -Dskip.license.check=true

# Switch to default user (UID 1001) at end of stage
USER 1001

# Stage 1: Build the HBase source code
FROM registry.access.redhat.com/ubi8/openjdk-17:1.23 AS build-stage

# Switch to user root (UID 0)
USER 0

# Install necessary build packages
RUN microdnf update -y && microdnf install -y \
        maven-1:3.8.5-6.module+el8.10.0+21301+657f54a3.noarch \
        git-2.43.7-1.el8_10.aarch64 \
        hostname-3.20-6.el8.aarch64 \
        diffutils-3.6-6.el8.aarch64 \
    && microdnf clean all

# Copy the cached Maven dependencies
COPY --from=cache-stage /root/.m2 /root/.m2

# Copy the HBase source code
COPY ./hbase /opt/hbase-src

WORKDIR /opt/hbase-src

# Build HBase source code using cached dependencies and enable parallel build
RUN mvn clean package -DskipTests -Dskip.license.check=true assembly:single -T 1C

# Switch to default user (UID 1001) at end of stage
USER 1001

# Stage 2: Create the final Docker image
FROM registry.access.redhat.com/ubi8/openjdk-17:1.23

# Switch to user root (UID 0)
USER 0

# Set environment variables
ENV HBASE_HOME=/opt/hbase
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
    HBASE_USER=hbase \
    HBASE_CONF_DIR=${HBASE_HOME}/conf \
    HBASE_LIB_DIR=${HBASE_HOME}/lib \
    HBASE_LOGS_DIR=${HBASE_HOME}/logs \
    DATA_DIR=/data-store

# Install necessary runtime packages
RUN microdnf update -y && microdnf install -y \
        unzip-6.0-48.el8_10.aarch64 \
        gzip-1.9-13.el8_5.aarch64 \
        wget-1.19.5-12.el8_10.aarch64 \
        hostname-3.20-6.el8.aarch64 \
        maven-1:3.8.5-6.module+el8.10.0+21301+657f54a3.noarch \
        git-2.43.7-1.el8_10.aarch64 \
        diffutils-3.6-6.el8.aarch64 \
        vim-common-2:8.0.1763-31.el8_10.aarch64 \
        vim-enhanced-2:8.0.1763-31.el8_10.aarch64 \
        vim-filesystem-2:8.0.1763-31.el8_10.noarch \
        openssh-clients-8.0p1-30.el8_10.aarch64 \
        python36-3.6.8-39.module+el8.10.0+20784+edafcd43.aarch64 \
        procps-ng-3.3.15-14.el8.aarch64 \
    && microdnf clean all

# Copy the built HBase binaries from the build-stage
COPY --from=build-stage /opt/hbase-src/hbase-assembly/target/hbase-*-bin.tar.gz /opt/hbase-dist/

# Extract the full HBase binary distribution (not the client-only assembly)
RUN archive="$(find /opt/hbase-dist -maxdepth 1 -type f -name 'hbase-*-bin.tar.gz' ! -name 'hbase-*-client-bin.tar.gz' -print -quit)" \
    && mkdir -p /opt/hbase \
    && tar -xzf "${archive}" -C /opt/hbase --strip-components=1 \
    && rm -rf /opt/hbase-dist

COPY --from=build-stage /root/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-common/*/hadoop-mapreduce-client-common-*.jar \
                        ${HBASE_HOME}/lib/

# Apply custom HBase build steps
RUN sed -i "s,^. export JAVA_HOME.*,export JAVA_HOME=$JAVA_HOME," ${HBASE_CONF_DIR}/hbase-env.sh \
    && sed -E -i 's/(.*)hbase\-daemons\.sh(.*zookeeper)/\1hbase-daemon.sh\2/g' ${HBASE_HOME}/bin/start-hbase.sh \
    && printf "JAVA_HOME=%s\nexport JAVA_HOME\nexport PATH=%s/jre/bin:%s\n" "$JAVA_HOME" "$JAVA_HOME" "$PATH" \
            > /etc/profile.d/defaults.sh \
    && ln -sf ${HBASE_HOME}/bin/* /usr/bin

# Create HBase user and home directory
RUN useradd -u 1000 -m ${HBASE_USER} \
    && mkdir -p /home/${HBASE_USER} \
    && chown -R ${HBASE_USER}:${HBASE_USER} /opt /home/${HBASE_USER} \
    && mkdir "$DATA_DIR" && chown -R ${HBASE_USER}:${HBASE_USER} "$DATA_DIR"

# Set permissions for jboss home directory
RUN chown -R ${HBASE_USER}:${HBASE_USER} /home/jboss

# Copy configuration files - Removed in order to mount config files individually #

# Expose required ports for HBase and related services
EXPOSE 8000 8080 8085 9090 9095 2181 16000 16010 16020 16030

# Set up the utils directory as a volume
VOLUME ["/opt/utils"]

# Switch to the HBase user (UID 1000)
USER 1000

# Create necessary directories for HBase to run
RUN mkdir -p "$DATA_DIR"/hbase "$DATA_DIR"/run "$DATA_DIR"/logs && \
    chmod -R 777 "$DATA_DIR"

# Add the 'ls -l' alias
RUN echo 'alias ll="ls -l"' >> /home/hbase/.bashrc && \
    echo 'alias ll="ls -l"' >> /home/jboss/.bashrc

# Start HBase and keep it running
ENTRYPOINT ["/bin/bash", "-c", "${HBASE_HOME}/bin/start-hbase.sh && tail -f ${HBASE_LOGS_DIR}/*.log"]
