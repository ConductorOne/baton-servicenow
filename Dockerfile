FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-servicenow"]
COPY baton-servicenow /