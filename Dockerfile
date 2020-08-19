FROM openjdk:11-jre

ENV VERTICLE_FILE search-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles
ENV SITEMAPS_HOME /usr/verticles/sitemaps

EXPOSE 8080 8081

RUN addgroup --system vertx && adduser --system --group vertx

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/
COPY conf/elasticsearch $VERTICLE_HOME/conf/elasticsearch
COPY conf/config.json.sample $VERTICLE_HOME/conf/config.json
COPY test $VERTICLE_HOME/test

RUN mkdir $SITEMAPS_HOME
RUN chown -R vertx:vertx $VERTICLE_HOME 
RUN chmod -R a+rwx $VERTICLE_HOME

USER vertx

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java $JAVA_OPTS -jar $VERTICLE_FILE"]
