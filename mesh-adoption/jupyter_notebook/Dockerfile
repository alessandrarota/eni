FROM jupyter/base-notebook

WORKDIR /home/jovyan

COPY ./notebook /home/jovyan

EXPOSE 8888

ENV JUPYTER_TOKEN=password

CMD ["start-notebook.sh", "--NotebookApp.token=${JUPYTER_TOKEN}"]
