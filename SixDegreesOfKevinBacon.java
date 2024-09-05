package ca.yorku.eecs;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.driver.v1.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SixDegreesOfKevinBacon implements HttpHandler {


    @Override
    public void handle(HttpExchange exchange) throws IOException {

        // Get request path and method
        String path = exchange.getRequestURI().getPath();
        String methodRequested = exchange.getRequestMethod();


        // Handle PUT or POST requests
        if ("PUT".equals(methodRequested) || "POST".equals(methodRequested)) {
            if ("/api/v1/addActor".equals(path)) {
                AddActorHandle(exchange);
            } else if ("/api/v1/addMovie".equals(path)) {
                AddMovieHandle(exchange);
            } else if ("/api/v1/addRelationship".equals(path)) {
                AddRelationshipHandle(exchange);
            } else {
                GetResponseStatus(exchange, 400, "Endpoint is not valid");
            }

        }

        // Handle GET requests
        else if ("GET".equals(methodRequested)) {

            if ("/api/v1/getActor".equals(path)) {
                getActorHandler(exchange);
            } else if ("/api/v1/getMovie".equals(path)) {
                getMovieHandler(exchange);
            } else if ("/api/v1/top10Actors".equals(path)) {
                top10ActorsHandler(exchange);
            } else if ("/api/v1/hasRelationship".equals(path)) {
                hasRelationshipHandler(exchange);
            } else if ("/api/v1/computeBaconNumber".equals(path)) {
                computeBaconNumberHandler(exchange);
            } else if ("/api/v1/computeBaconPath".equals(path)) {
                computeBaconPathHandler(exchange);
            } else if ("/api/v1/moviesByGenre".equals(path)) {
                moviesByGenreHandler(exchange);
            } else {
                GetResponseStatus(exchange, 400, "Endpoint is not valid");
            }
        }

        // Handle DELETE requests
        else if ("DELETE".equals(methodRequested)) {

            if ("/api/v1/deleteActor".equals(path)) {
                deleteActorHandler(exchange);
            } else {
                GetResponseStatus(exchange, 400, "Endpoint is not valid");
            }
        }

        // Invalid request method

        else {
            GetResponseStatus(exchange, 400, "Endpoint is not valid");
        }
    }


    /**
     * Adding a new actor endpoint
     *
     * @param name    Actor's name
     * @param actorId actorID  for the actor, which must be unique
     */

    private void addActor(String name, String actorId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "CREATE (a:Actor {actorId: $actorId, name: $name})";
            session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("actorId", actorId, "name", name)));
        }
    }


    /**
     * @param actorId
     * @return boolean if actor is unique or not based on actorId
     */
    private boolean IsActorNotUnique(String actorId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (a:Actor {actorId: $actorId}) RETURN a";
            return session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("actorId", actorId))).hasNext();

        }
    }


    /**
     * Handle HTTP request for Adding Actor. Get incoming Request extracts the name and actorId and based on
     * checks attemps to add the Node
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    private void AddActorHandle(HttpExchange exchange) throws IOException {
        try {
            String body = RequestBodyReader(exchange.getRequestBody());


            // Parse the request body to a JSONObject
            JSONObject deserialized = new JSONObject(body);

            // get the actor name and actor ID from the JSON body

            String name = deserialized.has("name") ? deserialized.getString("name") : null;
            String actorId = deserialized.has("actorId") ? deserialized.getString("actorId") : null;


            // ensure both name and actor are not null and empty
            if ((name != null && actorId != null) && (!name.isEmpty() && !actorId.isEmpty())) {

                // Check if the actor already exists - response 400 status if actor exist
                if (IsActorNotUnique(actorId)) {
                    GetResponseStatus(exchange, 400, "Actor already exists");
                } else {
                    try {
                        // successful add of actor - response with a 200 status
                        addActor(name, actorId);
                        GetResponseStatus(exchange, 200, "Actor successfully added");
                    } catch (Exception e) {
                        // fail to add actor - response 500 status
                        GetResponseStatus(exchange, 500, "Actor failed to add");
                    }
                }
            } else {
                // Respond 400  if the JSON format is invalid or missing info
                GetResponseStatus(exchange, 400, "JSON format not valid");
            }
        } catch (JSONException e) {
            // Handle JSON parsing errors
            e.printStackTrace();
            // Respond 400  if the JSON format is invalid or missing info
            GetResponseStatus(exchange, 400, "JSON format not valid");
        }
    }


    /**
     * Adding a new movie endpoint
     *
     * @param name    Movie's name
     * @param movieId movieId  for the movie, which must be unique
     */

    public void addMovie(String name, String movieId, String genre) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "CREATE (m:Movie {movieId: $movieId, name: $name, genre: $genre})";
            session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("movieId", movieId,
                    "name", name, "genre", genre)));
        }
    }

    /**
     * @param movieId
     * @return boolean if movie is unique or not based on movieId
     */
    private boolean IsMovieNotUnique(String movieId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (m:Movie {movieId: $movieId}) RETURN m";
            return session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("movieId", movieId))).hasNext();

        }
    }


    /**
     * Handle HTTP request for Adding Movie. Get incoming Request extracts the name and movieId and based on
     * checks attemps to add the Node
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    private void AddMovieHandle(HttpExchange exchange) throws IOException {
        try {
            String body = RequestBodyReader(exchange.getRequestBody());

            // Parse the request body to a JSONObject
            JSONObject deserialized = new JSONObject(body);

            // get the actor name and movie ID and genre from the JSON body
            String name = deserialized.has("name") ? deserialized.getString("name") : null;
            String movieId = deserialized.has("movieId") ? deserialized.getString("movieId") : null;
            String genre = deserialized.has("genre") ? deserialized.getString("genre") : null;

            if ((name != null && movieId != null && genre != null) && (!name.isEmpty() && !movieId.isEmpty() && !genre.isEmpty())) {
                // Check if the movie already exists - response 400 status if movie exist

                if (IsMovieNotUnique(movieId)) {
                    GetResponseStatus(exchange, 400, "Movie already exists");
                } else {
                    try {
                        // successful add of movie - response with a 200 status
                        addMovie(name, movieId, genre);
                        GetResponseStatus(exchange, 200, "Movie successfully added");
                    } catch (Exception e) {
                        // fail to add movie - response 500 status
                        GetResponseStatus(exchange, 500, "Movie failed to add");
                    }
                }
            } else {
                // Respond 400  if the JSON format is invalid or missing info
                GetResponseStatus(exchange, 400, "JSON format not valid");
            }
        } catch (JSONException e) {
            e.printStackTrace();
            // Respond 400  if the JSON format is invalid or missing info
            GetResponseStatus(exchange, 400, "JSON format not valid");
        }
    }


    /**
     * Adds a relationship between an actor and a movie
     * <p>
     * Makes an "ACTED_IN" relationship between the  actor and movie
     * if both entities exist and relatioship is not already created.
     * </p>
     *
     * @param movieId The unique for each  movie.
     * @param actorId The unique for each actor.
     */
    public void addRelationship(String movieId, String actorId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (a:Actor), (m:Movie) " +
                    "WHERE a.actorId = $actorId AND m.movieId = $movieId" +
                    " CREATE (a)-[r:ACTED_IN]->(m) " +
                    "RETURN type(r);";
            session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("actorId", actorId,
                    "movieId", movieId)));
        }
    }


    /**
     * Checks if there is an actor with actorID already in database
     * <p>
     * If actor exist return true else false
     * </p>
     *
     * @param actorId unique for each actor
     * @return true if the actor exists, false otherwise.
     */
    private boolean doesActorExist(String actorId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (a:Actor {actorId: $actorId}) RETURN a";
            return session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("actorId", actorId))).hasNext();

        }
    }


    /**
     * Checks if there is an movie with movieId already in database
     * <p>
     * If movieID exist return true else false
     * </p>
     *
     * @param movieId unique for each actor
     * @return true if the movie exists, false otherwise.
     */
    private boolean doesMovieExist(String movieId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (m:Movie {movieId: $movieId}) RETURN m";
            return session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("movieId", movieId))).hasNext();

        }
    }

    private boolean IsRelatioshipNotUnique(String actorId, String movieId) {
        try (Session session = Utils.getSession()) {
            String statementTemplate = "MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie) WHERE a.actorId = $actorId AND m.movieId = $movieId RETURN r;";
            return session.writeTransaction(tx -> tx.run(statementTemplate, Values.parameters("actorId", actorId, "movieId", movieId))).hasNext();

        }
    }


    /**
     * Handle HTTP request for Adding relationship. Get incoming Request extracts the actorId and movieId and based on
     * checks attempts to add the Node
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    public void AddRelationshipHandle(HttpExchange exchange) throws IOException {
        try {

            String body = RequestBodyReader(exchange.getRequestBody());

            // deserialize the JSON body
            JSONObject deserialized = new JSONObject(body);
            String movieId = deserialized.has("movieId") ? deserialized.getString("movieId") : null;
            String actorId = deserialized.has("actorId") ? deserialized.getString("actorId") : null;


            // Check if both movieId and actorId are not null and empty
            if ((movieId != null && actorId != null) && (!movieId.isEmpty() && !actorId.isEmpty())) {


                // check if given actor and movie Id already exist
                if (doesActorExist(actorId) && doesMovieExist(movieId)) {

                    // Response 400 if relationship already exist in the database
                    if (IsRelatioshipNotUnique(actorId, movieId)) {
                        GetResponseStatus(exchange, 400, "Relationship ACTED_IN already exist");
                    } else {
                        try {
                            // Response 200 if relationship ACTED_IN relationship created
                            addRelationship(movieId, actorId);
                            GetResponseStatus(exchange, 200, "Relationship ACTED_IN successfully added");
                        } catch (IOException e) {

                            // Not able to add - response 500
                            GetResponseStatus(exchange, 500, "Relationship ACTED_IN failed to add");
                        }
                    }
                }
                // Response 404 if actor or movie does not exist in database
                else {
                    GetResponseStatus(exchange, 404, "Actor/Movie does not exist");
                }

            }
            // Respond 400  if the JSON format is invalid or missing info
            else {
                GetResponseStatus(exchange, 400, "JSON format not valid");
            }

        } catch (JSONException e) {
            e.printStackTrace();
            // Respond 400  if the JSON format is invalid or missing info
            GetResponseStatus(exchange, 400, "JSON format not valid");
        }
    }


    /**
     * Handle HTTP GET request getting the actor
     * IT fetches actor Based on actorID parameter, if actorId matches then it returns actorId, name, and the
     * ACTED_IN relationship they have with all the movies Node
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    public void getActorHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {


                // Fetch query parameters from the request URI
                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String actorId = params.get("actorId");

                // Validate that the actorId parameter is is not null and empty
                if (actorId == null || actorId.isEmpty()) {
                    sendResponse(request, 400, "actorId is required");
                } else {
                    // Neo4j query to get actor details and  movies they have ACTED_IN

                    try (Session session = Utils.getSession()) {
                        String queryString = "MATCH (a:Actor) " +
                                "WHERE a.actorId = $actorId " +
                                "OPTIONAL MATCH (a)-[r:ACTED_IN]->(m:Movie) " +
                                "RETURN a.name as name, a.actorId as actorId, collect(m.name) as movies";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("actorId", actorId);

                        StatementResult result = session.run(queryString, parameters);


                        // If actor not found in database - response 404
                        if (!result.hasNext()) {
                            sendResponse(request, 404, "Actor not found");
                        } else {
                            List<String> movies = new ArrayList<>();
                            String actorName = null;

                            while (result.hasNext()) {
                                Record record = result.next();
                                actorName = record.get("name").asString();
                                List<Object> movieObjects = record.get("movies").asList();
                                for (Object obj : movieObjects) {
                                    movies.add(obj.toString());
                                }
                            }

                            Map<String, Object> response = new HashMap<>();
                            response.put("actorId", actorId);
                            response.put("name", actorName);
                            response.put("movies", movies);

                            // actor successfully found response 200
                            sendResponse(request, 200, Utils.toJson(response));


                        }

                    }
                    // unexpected error 500 response
                    catch (Exception e) {
                        sendResponse(request, 500, "Internal server error");
                    }

                }
            } else {
                // Respond 400  if the JSON format is invalid or missing info
                sendResponse(request, 400, "JSON format not valid");
            }
        } catch (Exception e) {
            // Respond 400  if the JSON format is invalid or missing info
            sendResponse(request, 400, "JSON format not valid");
        }
    }


    /**
     * Handle HTTP GET request getting the movie
     * IT fetches movie Based on movieId parameter, if movieId matches then it returns movieId, name, and the
     * ACTED_IN relationship they have with all the actor Node
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    private void getMovieHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {

                // Fetch query parameters from the request URI

                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String movieId = params.get("movieId");

                if (movieId == null || movieId.isEmpty()) {
                    sendResponse(request, 400, "movieId is required");
                } else {
                    try (Session session = Utils.getSession()) {
                        String queryString = "MATCH (m:Movie) " +
                                "WHERE m.movieId = $movieId " +
                                "OPTIONAL MATCH (m)<-[r:ACTED_IN]-(a:Actor) " +
                                "RETURN m.name as name, m.movieId as movieId, collect(a.name) as actors";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("movieId", movieId);

                        StatementResult result = session.run(queryString, parameters);

                        // If movieId not found in database - response 404
                        if (!result.hasNext()) {
                            sendResponse(request, 404, "Movie not found");
                        } else {
                            List<String> actors = new ArrayList<>();
                            String movieName = null;

                            while (result.hasNext()) {
                                Record record = result.next();
                                movieName = record.get("name").asString();
                                List<Object> actorObjects = record.get("actors").asList();
                                for (Object obj : actorObjects) {
                                    actors.add(obj.toString());
                                }

                            }

                            Map<String, Object> response = new HashMap<>();
                            response.put("movieId", movieId);
                            response.put("name", movieName);
                            response.put("actors", actors);

                            sendResponse(request, 200, Utils.toJson(response));


                        }

                    } catch (Exception e) {
                        // unexpected error 500 response
                        sendResponse(request, 500, "Internal server error");
                    }

                }
            } else {
                // Respond 400  if the JSON format is invalid or missing info
                sendResponse(request, 400, "JSON format not valid");
            }
        } catch (Exception e) {
            // Respond 400  if the JSON format is invalid or missing info
            sendResponse(request, 400, "JSON format not valid");
        }
    }


    /**
     * Handle HTTP GET request getting the relationship
     * IT fetches the relationship between actor and movie
     * parameter actorID and movieID are passed
     * <p>
     * Returns response body as actorID, movieID and boolean to ensure if relationship exist or not
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */

    public void hasRelationshipHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {

                // Fetch query parameters from the request URI
                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);


                String actorId = params.get("actorId");
                String movieId = params.get("movieId");


                // ensure that both actorId and movieId are not empty and not null - else response 404
                if (actorId == null || actorId.isEmpty() || movieId == null || movieId.isEmpty()) {


                    sendResponse(request, 404, "actorId and movieId are required");
                } else {
                    try (Session session = Utils.getSession()) {
                        // query to check if relationship exist between actor and movie node
                        String queryString = "MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie) " +
                                "WHERE a.actorId = $actorId AND m.movieId = $movieId " +
                                "RETURN count(r) as relationship";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("actorId", actorId);
                        parameters.put("movieId", movieId);

                        StatementResult result = session.run(queryString, parameters);

                        if (result.hasNext()) {
                            // if relatioship exist returns greater than 0 value
                            boolean hasRelationship = result.next().get("relationship").asInt() > 0;
                            if (hasRelationship) {
                                Map<String, Object> response = new HashMap<>();
                                response.put("actorId", actorId);
                                response.put("movieId", movieId);
                                response.put("hasRelationship", hasRelationship);
                                sendResponse(request, 200, Utils.toJson(response));
                            } else {
                                // 404 , if actor/movie does not exist or relationship does not exist
                                sendResponse(request, 404, "Movie/Actor not found or relationship does not exist");
                            }
                        } else {
                            // response 404 if not relationship exist
                            sendResponse(request, 404, "No relationship found");
                        }
                    } catch (Exception e) {
                        // 500 response
                        sendResponse(request, 500, "Internal server error");
                    }
                }
            } else {

                // response 400 for json format issue or info missing
                sendResponse(request, 400, "Invalid request method");
            }
        } catch (Exception e) {
            // response 400 for json format issue or info missing
            sendResponse(request, 400, "Invalid request format");
        }
    }


    /**
     * Handle HTTP GET request getting the bacon Number
     * Computes the bacon number from given actorID parameter to Kevin bacon
     * nm0000102. Determines the shortest path from given actor to kevin Bacon
     * Returns the path divide by 2 as dividing 2 ensure we get the path only from actor to actor
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    public void computeBaconNumberHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {


                // parameter actorID
                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String actorId = params.get("actorId");


                // if actorID is empty or null then response 400 for missing information
                if (actorId == null || actorId.isEmpty()) {
                    sendResponse(request, 400, "actorId is required");
                } else {
                    try (Session session = Utils.getSession()) {
                        // query to find the shortest path from given actor to kevin bacon
                        String queryString = "MATCH (a:Actor {actorId: $actorId}), (d:Actor {actorId: 'nm0000102'}) "
                                + "RETURN CASE "
                                + "    WHEN a.actorId = d.actorId THEN 0 "
                                + "    ELSE length(shortestPath((a)-[:ACTED_IN*]-(d))) "
                                + "END AS baconNumber";


                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("actorId", actorId);

                        StatementResult result = session.run(queryString, parameters);

                        if (result.hasNext()) {

                            Record record = result.next();

                            // if there is no given actor or actor has not path to kevin bacon response 404
                            if (record.get("baconNumber").isNull()) {
                                sendResponse(request, 404, "Actor not found or no path to Kevin Bacon");
                            } else {

                                // if actor path is found response 200 with their bacon number
                                int baconResult = record.get("baconNumber").asInt();
                                int baconNumber = (baconResult == 0) ? 0 : baconResult / 2;
                                sendResponse(request, 200, "Bacon Number: " + baconNumber);
                            }

                        } else {
                            // if there is no given actor or actor has not path to kevin bacon response 404
                            sendResponse(request, 404, "Actor not found or no path to Kevin Bacon");
                        }
                    } catch (Exception e) {
                        // fail and server error response 500
                        sendResponse(request, 500, "Internal server error");
                    }
                }
            } else {
                // response 400 for JSON format issue or info missing
                sendResponse(request, 400, "Invalid request method");
            }
        } catch (Exception e) {
            // response 400 for JSON format issue or info missing
            sendResponse(request, 400, "Invalid request format");
        }
    }


    /**
     * Handle HTTP GET request getting the bacon path
     * Computes the bacon path from given actorID parameter to Kevin bacon
     * nm0000102. Determines the shortest path from given actor to kevin Bacon
     * Returns the path divide by 2 as dividing 2 ensure we get the path only from actor to actor
     *
     * Response body gives the shortest path from actor to Kevin bacon return all the actor within those path
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    public void computeBaconPathHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {
                // parameter actorID
                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String actorId = params.get("actorId");


                // if actorID is empty or null then response 400 for missing information
                if (actorId == null || actorId.isEmpty()) {
                    sendResponse(request, 400, "actorId is required");
                } else {
                    try (Session session = Utils.getSession()) {


                        // query to find the shortest path from given actor to kevin bacon and get the actorID list
                        String queryString = "MATCH (a:Actor {actorId: $actorId}), (d:Actor {actorId: 'nm0000102'}) "
                                + "WITH a, d "
                                + "RETURN CASE "
                                + "    WHEN a.actorId = d.actorId THEN ['nm0000102'] "
                                + "    ELSE CASE "
                                + "        WHEN shortestPath((a)-[:ACTED_IN*]-(d)) IS NULL THEN [] "
                                + "        ELSE [m IN nodes(shortestPath((a)-[:ACTED_IN*]-(d))) WHERE m.actorId IS NOT NULL | m.actorId] "
                                + "    END "
                                + "END AS baconPath";


                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("actorId", actorId);

                        StatementResult result = session.run(queryString, parameters);

                        if (result.hasNext()) {
                            List<String> baconPath = result.next().get("baconPath").asList(Value::asString);


                            // If it's empty then the Actor has no path to Kevin Bacon
                            if (baconPath.isEmpty()) {
                                sendResponse(request, 404, "Actor not found or no path to Kevin Bacon");

                            } else {
                                // if actor path is found response 200 and all the actorID path list
                                String jsonResponse = "{\"baconPath\": " + baconPath.toString() + "}";
                                sendResponse(request, 200, jsonResponse);
                            }


                        }

                        // if there is no given actor or actor has not path to kevin bacon response 404
                        else {
                            sendResponse(request, 404, "Actor not found or no path to Kevin Bacon");
                        }
                    } catch (Exception e) {
                        sendResponse(request, 500, "Internal server error");
                    }
                }
            } else {
                // response 400 for JSON format issue or info missing
                sendResponse(request, 400, "Invalid request method");
            }
        } catch (Exception e) {
            // response 400 for JSON format issue or info missing
            sendResponse(request, 400, "Invalid request format");
        }
    }


    /**
     * Handle HTTP GET request getting the movies based on genre
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    public void moviesByGenreHandler(HttpExchange request) throws IOException {
        try {
            if ("GET".equals(request.getRequestMethod())) {

                // requires parameter genre
                String body = request.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String genre = params.get("genre");


                // response 400 to ensure parameter is not null and empty
                if (genre == null || genre.isEmpty()) {
                    sendResponse(request, 400, "genre is required");
                } else {
                    try (Session session = Utils.getSession()) {
                        // query check and returns a list of movies based on the parameter genre
                        String queryString = "MATCH (m:Movie) " +
                                "WHERE $genre IN m.genre " +
                                "RETURN m.name as name, m.movieId as movieId";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("genre", genre);

                        StatementResult result = session.run(queryString, parameters);


                        // response 404 if no movies found for given genre
                        if (!result.hasNext()) {
                            sendResponse(request, 404, "No movies found for the given genre");
                        } else {
                            List<Map<String, String>> movies = new ArrayList<>();
                            while (result.hasNext()) {
                                Record record = result.next();
                                Map<String, String> movie = new HashMap<>();
                                movie.put("name", record.get("name").asString());
                                movie.put("movieId", record.get("movieId").asString());
                                movies.add(movie);
                            }
                            // response 200 for successfully finding at least one movie for given genre
                            sendResponse(request, 200, movies.toString());
                        }
                    } catch (Exception e) {
                        sendResponse(request, 500, "Internal server error");
                    }
                }
            } else {
                // response 400 for JSON format issue or info missing
                sendResponse(request, 400, "Invalid request method");
            }
        } catch (Exception e) {
            // response 400 for JSON format issue or info missing
            sendResponse(request, 400, "Invalid request format");
        }
    }


    /**
     * Handle getting at most 10 actors who has the the relationship of most ACTED_IN in order
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    private void top10ActorsHandler(HttpExchange exchange) throws IOException {
        try {
            if ("GET".equals(exchange.getRequestMethod())) {
                // ensure the request body is empty
                if (exchange.getRequestBody().available() > 0) {
                    sendResponse(exchange, 400, "Request body must be empty");
                    return;
                }


                try (Session session = Utils.getSession()) {
                    // query to get the actor who has acted_IN relationship with the most movies
                    // limit to 10 actors
                    String queryString = "MATCH (a:Actor)-[r:ACTED_IN]-> (m:Movie) " +
                            "RETURN a.name as name, COUNT(m) as totalMovies " +
                            "ORDER BY totalMovies DESC " +
                            "LIMIT 10";

                    StatementResult result = session.run(queryString);


                    // if there exist no actors who has an ACTED_IN relationship response 400
                    if (!result.hasNext()) {
                        sendResponse(exchange, 404, "No actors found");
                    } else {
                        Map<String, Object> response = new HashMap<>();
                        List<Map<String, Object>> actorInfo = new ArrayList<>();


                        while (result.hasNext()) {
                            Record record = result.next();
                            Map<String, Object> m = new HashMap<>();
                            m.put("name", record.get("name").asString());
                            m.put("totalMovies", record.get("totalMovies").asInt());
                            actorInfo.add(m);

                        }

                        // response 200 , found at least 1 or more actor who have ACTED_IN relationship
                        response.put("Top 10 Actors", actorInfo);
                        sendResponse(exchange, 200, Utils.toJson(response));
                    }

                } catch (Exception e) {
                    sendResponse(exchange, 500, "Internal server error");
                }
            } else {
                // response 400 for JSON format issue or info missing
                sendResponse(exchange, 400, "JSON format not valid");
            }
        } catch (Exception e) {
            // response 400 for JSON format issue or info missing
            sendResponse(exchange, 400, "JSON format not valid");
        }
    }


    /**
     * Handle delete method - delete the actor based on the given actorId , if actor exist
     *
     * @param exchange HttpExchange object which has  response and request value
     * @throws IOException to ensure if there  input or output exception occurs during handling of the request
     */
    private void deleteActorHandler(HttpExchange exchange) throws IOException {
        Transaction tx = null;
        Session session = null;
        try {

            if ("DELETE".equals(exchange.getRequestMethod())) {


                // parameter actorID
                String body = exchange.getRequestURI().getQuery();
                Map<String, String> params = parseQuery(body);
                String actorId = params.get("actorId");


                // if actorID is empty or null response 400
                if (actorId == null || actorId.isEmpty()) {
                    sendResponse(exchange, 400, "actorId is required");
                } else {
                    try {
                        session = Utils.getSession();
                        tx = session.beginTransaction();

                        // Check if the actor exists in the database
                        String checkQuery = "MATCH (a:Actor {actorId: $actorId}) RETURN a";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("actorId", actorId);

                        StatementResult checkResult = tx.run(checkQuery, parameters);


                        // if no actor found response 404
                        if (!checkResult.hasNext()) {
                            sendResponse(exchange, 404, "Actor not found");
                            tx.success();
                            ;
                        } else {
                            // query check if actor exist based on ID, if it exist it delete it
                            // if actor NOde has relationship it removes it and delete the actor node
                            String queryString = "MATCH (a:Actor {actorId: $actorId}) DETACH DELETE a";

                            tx.run(queryString, parameters);
                            tx.success();


                            // deletes actor successfully
                            sendResponse(exchange, 200, "Actor deleted successfully");
                        }
                    } catch (Exception e) {
                        // response 500 for fail to delete or server error
                        sendResponse(exchange, 500, "Internal server error");
                    }
                }

            } else {
                // response 400 for JSON format issue or info missing
                sendResponse(exchange, 400, "JSON format not valid");
            }
        } catch (Exception e) {
            // response 400 for JSON format issue or info missing
            sendResponse(exchange, 400, "JSON format not valid");
        }
    }


    /**
     * Gives an HTTP response with the corresponding status code and response body required
     *
     * @param exchange   HttpExchange object representing the  request
     * @param statusCode status code for HTTPS  to send
     * @param response   the actual body to sent to the user
     * @throws IOException If exception error occurs while passing the response
     */

    private void GetResponseStatus(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.sendResponseHeaders(statusCode, response.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }


    /**
     * Does the job of reading the request body from an InputStream and string is returned
     *
     * @param exchange   HttpExchange object representing the  request
     * @param statusCode status code for HTTPS  to send
     * @param response   the actual body to sent to the user
     * @throws IOException If exception error occurs while passing the response
     */
    private String RequestBodyReader(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }


    private static Map<String, String> parseQuery(String query) throws UnsupportedEncodingException {
        Map<String, String> queryPairs = new HashMap<>();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0 && idx < pair.length() - 1) {
                String key = pair.substring(0, idx);
                String value = pair.substring(idx + 1);
                queryPairs.put(key, value);
            }
        }
        return queryPairs;
    }

    /**
     * Parses the query string of a URL and returns a map which has a key and value pair
     *
     * @param query string from a URL
     * @return A map which contains  parameters
     * @throws If unsupported character isusse.
     */
    private static void sendResponse(HttpExchange request, int statusCode, String response) throws IOException {
        request.sendResponseHeaders(statusCode, response.getBytes(StandardCharsets.UTF_8).length);

        try (OutputStream os = request.getResponseBody()) {
            os.write(response.getBytes(StandardCharsets.UTF_8));
        }
    }


}
