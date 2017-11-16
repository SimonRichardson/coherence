FROM iron/go

EXPOSE 8080

WORKDIR /app
ADD coherence /app/

ENTRYPOINT ["./coherence"]
