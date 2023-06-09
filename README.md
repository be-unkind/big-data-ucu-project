## Project for Big Data course (UCU)

---

### Authors:
- Anastasiia Havryliv
- Yaroslav Romanus

---

### Aim of the project
The aim of this project is to implement a system, which will process [Wikipedia stream](https://stream.wikimedia.org/v2/stream/page-create) data and process it for further access to it from REST API endpoints. Several technologies were used in the project, including Kafka, Cassandra, Spark (for batch processing), Spark Streaming and FastAPI.

---

### System Design (Description)

System design diagram and detailed description of all of the components can be found [here](https://github.com/be-unkind/big-data-ucu-project/blob/main/System-Description.pdf)

---

### Project results

Results from endpoint requests along with demonstation picture of running system in Docker (working containers) can be found [here](https://github.com/be-unkind/big-data-ucu-project/tree/main/results)

{ more detailed urls:

- [precomuted reports](https://github.com/be-unkind/big-data-ucu-project/tree/main/results/precomputed)
- [ad-hoc queries](https://github.com/be-unkind/big-data-ucu-project/tree/main/results/ad_hoc)
- [general endpoints docs](https://github.com/be-unkind/big-data-ucu-project/blob/main/results/rest_api_docs.pdf)

---

### How to test the system (how to run and shut it down)

To run the system (write in terminal):

`docker-compose up`

To shut the system down (write in terminal):

`docker-compose down`
