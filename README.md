# sparkdbserver
This project is to use spark driver as database server, provide Spark & HDFS functionalities like the way database uses. Virtually this project let user develop Java program with big data technology as relational database.

In the client side, user can send out request for data resource; server(Spark driver) listen to client's request, runs Spark transformations and actions, then returns calculation result to client.

Current code is a simple example to show how this mechanism woks, later further developments will make it more useful and practical.
