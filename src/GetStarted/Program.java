package GetStarted;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.codehaus.groovy.runtime.StringGroovyMethods;

import javax.management.Query;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class Program
{
//    static final String gremlinQueries[] = new String[] {
//                        "g.V().drop()",
//            "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44).property('pk', 'pk')",
//            "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39).property('pk', 'pk')",
//            "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller').property('pk', 'pk')",
//            "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield').property('pk', 'pk')",
//            "g.V('thomas').addE('knows').to(g.V('mary'))",
//            "g.V('thomas').addE('knows').to(g.V('ben'))",
//            "g.V('ben').addE('knows').to(g.V('robin'))",
//            "g.V('thomas').property('age', 44)",
//            "g.V().count()",
//            "g.V().hasLabel('person').has('age', gt(40))",
//            "g.V().hasLabel('person').order().by('firstName', decr)",
//            "g.V('thomas').outE('knows').inV().hasLabel('person')",
//            "g.V('thomas').outE('knows').inV().hasLabel('person').outE('knows').inV().hasLabel('person')",
//            "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()",
//            "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()"};

//
//  public static void main2( String[] args ) throws ExecutionException, InterruptedException {
//
//
//
//        /**
//         * There typically needs to be only one Cluster instance in an application.
//         */
//        Cluster cluster;
//
//        /**
//         * Use the Cluster instance to construct different Client instances (e.g. one for sessionless communication
//         * and one or more sessions). A sessionless Client should be thread-safe and typically no more than one is
//         * needed unless there is some need to divide connection pools across multiple Client instances. In this case
//         * there is just a single sessionless Client instance used for the entire App.
//         */
//        Client client;
//
//        try {
//            // Attempt to create the connection objects
//            Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
//            cluster = temp.create();
//            client = cluster.connect();
//        } catch (FileNotFoundException e) {
//            // Handle file errors.
//            System.out.println("Couldn't find the configuration file.");
//            e.printStackTrace();
//            return;
//        }
//
//        // After connection is successful, run all the queries against the server.
//        for (String query : gremlinQueries) {
//            System.out.println("\nSubmitting this Gremlin query: " + query);
//
//            // Submitting remote query to the server.
//            ResultSet results = client.submit(query);
//            CompletableFuture<List<Result>> completableFutureResults;
//            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
//            List<Result> resultList;
//            Map<String, Object> statusAttributes;
//
//            try{
//                completableFutureResults = results.all();
//                completableFutureStatusAttributes = results.statusAttributes();
//                resultList = completableFutureResults.get();
//                statusAttributes = completableFutureStatusAttributes.get();
//            }
//            catch(ExecutionException | InterruptedException e){
//                e.printStackTrace();
//                break;
//            }
//            catch(Exception e){
//                ResponseException re = (ResponseException) e.getCause();
//
//                // Response status codes. You can catch the 429 status code response and work on retry logic.
//                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
//                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));
//
//                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
//                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));
//
//                // Total Request Units (RUs) charged for the operation, upon failure.
//                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));
//
//                // ActivityId for server-side debugging
//                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
//                throw(e);
//            }
//
//            for (Result result : resultList) {
//                System.out.println("\nQuery result:");
//                System.out.println(result.toString());
//            }
//
//            // Status code for successful query. Usually HTTP 200.
//            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());
//
//            // Total Request Units (RUs) charged for the operation, after a successful run.
//            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
//        }
//
//        System.out.println("Demo complete!\n Press Enter key to continue...");
//        try{
//            System.in.read();
//        } catch (IOException e){
//            e.printStackTrace();
//            return;
//        }
//
//        // Properly close all opened clients and the cluster
//        cluster.close();
//
//        System.exit(0);
//    }

    public static void init(){

//        try {
//            // Attempt to create the connection objects
//            Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
//            cluster = temp.create();
//            client = cluster.connect();
//        } catch (FileNotFoundException e) {
//            // Handle file errors.
//            System.out.println("Couldn't find the configuration file.");
//            e.printStackTrace();
//            return;
//        }
    }


    public static void main( String[] args ) throws ExecutionException, InterruptedException {
      initGraph();
//
//      // add V
//      Vertex v = new Vertex();
//      v.setId("joker");
//      v.setLabel("person");
//      v.getProperties().put("firstname", "ff");
//      v.getProperties().put("lastname", "ll");
//      v.getProperties().put("age", 44);
//      List<Vertex> vertices = Arrays.asList(v);
//      addV(v);
//
      //add E
      Vertex v_1 = new Vertex();
      v_1.setId("somebody");
      v_1.setLabel("person");
      v_1.getProperties().put("name", "ff");
      v_1.getProperties().put("age", 30);
      v_1.getProperties().put("work position", "PVG06 A4");
      v_1.getProperties().put("Risk of infection", "次密");
        Vertex v_2 = new Vertex();
        v_2.setId("Jay");
        v_2.setLabel("person");
        v_2.getProperties().put("name", "Gu Jay");
        v_2.getProperties().put("age", 30);
        v_2.getProperties().put("work position", "PVG06 A4");
        v_2.getProperties().put("Risk of infection", "次次密");
        Vertex v_3 = new Vertex();
        v_3.setId("Young");
        v_3.setLabel("person");
        v_3.getProperties().put("name", "Young");
        v_3.getProperties().put("age", 25);
        v_3.getProperties().put("work position", "PVG06 C8");
        v_3.getProperties().put("Risk of infection", "次次次密");
        Vertex v_4 = new Vertex();
        v_4.setId("Lucky");
        v_4.setLabel("person");
        v_4.getProperties().put("name", "Lucky");
        v_4.getProperties().put("age", 25);
        v_4.getProperties().put("work position", "PVG06 C8");
        v_4.getProperties().put("Risk of infection", "次次次密");
        Vertex v_5 = new Vertex();
        v_5.setId("William");
        v_5.setLabel("person");
        v_5.getProperties().put("name", "William");
        v_5.getProperties().put("age", 25);
        v_5.getProperties().put("work position", "PVG06 C8");
        v_5.getProperties().put("Risk of infection", "次次次密");
      Edge edge = new Edge();
      edge.setLabel("Touch");
      edge.getProperties().put("time", "2021-11-25");
      addV(v_1);
      addV(v_2);
      addV(v_3);
      addV(v_4);
      addV(v_5);
      addE(v_1, v_2, edge);
      addE(v_2, v_3, edge);
      addE(v_2, v_4, edge);
      addE(v_2, v_5, edge);

      // find vertex by id
        Vertex vertex = findVertexById("Jay");
    }


    public static void initGraph() throws ExecutionException, InterruptedException {

        String singleQuery = "g.V().drop()";

        String gremlinQueries[] = new String[] {singleQuery};

        Cluster cluster;
        Client client;
        try {
            // Attempt to create the connection objects
            Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
            cluster = temp.create();
            client = cluster.connect();
        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return;
        }

        // After connection is successful, run all the queries against the server.
        for (String query : gremlinQueries) {
            System.out.println("\nSubmitting this Gremlin query: " + query);

            // Submitting remote query to the server.
            ResultSet results = client.submit(query);
            CompletableFuture<List<Result>> completableFutureResults;
            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
            List<Result> resultList;
            Map<String, Object> statusAttributes;

            try{
                completableFutureResults = results.all();
                completableFutureStatusAttributes = results.statusAttributes();
                resultList = completableFutureResults.get();
                statusAttributes = completableFutureStatusAttributes.get();
            }
            catch(ExecutionException | InterruptedException e){
                e.printStackTrace();
                break;
            }
            catch(Exception e){
                ResponseException re = (ResponseException) e.getCause();

                // Response status codes. You can catch the 429 status code response and work on retry logic.
                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));

                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

                // Total Request Units (RUs) charged for the operation, upon failure.
                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));

                // ActivityId for server-side debugging
                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
                throw(e);
            }

            for (Result result : resultList) {
                System.out.println("\nQuery result:");
                System.out.println(result.toString());
            }

            // Status code for successful query. Usually HTTP 200.
            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

            // Total Request Units (RUs) charged for the operation, after a successful run.
            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
        }

        System.out.println("Demo complete!\n Press Enter key to continue...");
//        try{
//            System.in.read();
//        } catch (IOException e){
//            e.printStackTrace();
//            return;
//        }

        // Properly close all opened clients and the cluster
        cluster.close();

        //System.exit(0);
    }


    public static void addV(Vertex v) throws ExecutionException, InterruptedException {

      String singleQuery = "g.addV('"+v.getLabel()+"').property('id', '"+v.getId()+"')";
      for(Object key:v.getProperties().keySet()){
          singleQuery += ".property('"+key+"', '"+v.getProperties().get(key)+"')";
      }
      singleQuery += ".property('pk', 'pk')";
      String gremlinQueries[] = new String[] {singleQuery};

      Cluster cluster;
      Client client;
      try {
            // Attempt to create the connection objects
            Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
            cluster = temp.create();
            client = cluster.connect();
        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return;
        }

        // After connection is successful, run all the queries against the server.
        for (String query : gremlinQueries) {
            System.out.println("\nSubmitting this Gremlin query: " + query);

            // Submitting remote query to the server.
            ResultSet results = client.submit(query);
            CompletableFuture<List<Result>> completableFutureResults;
            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
            List<Result> resultList;
            Map<String, Object> statusAttributes;

            try{
                completableFutureResults = results.all();
                completableFutureStatusAttributes = results.statusAttributes();
                resultList = completableFutureResults.get();
                statusAttributes = completableFutureStatusAttributes.get();
            }
            catch(ExecutionException | InterruptedException e){
                e.printStackTrace();
                break;
            }
            catch(Exception e){
                ResponseException re = (ResponseException) e.getCause();

                // Response status codes. You can catch the 429 status code response and work on retry logic.
                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));

                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

                // Total Request Units (RUs) charged for the operation, upon failure.
                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));

                // ActivityId for server-side debugging
                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
                throw(e);
            }

            for (Result result : resultList) {
                System.out.println("\nQuery result:");
                System.out.println(result.toString());
            }

            // Status code for successful query. Usually HTTP 200.
            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

            // Total Request Units (RUs) charged for the operation, after a successful run.
            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
        }

        System.out.println("Demo complete!\n Press Enter key to continue...");
//        try{
//            System.in.read();
//        } catch (IOException e){
//            e.printStackTrace();
//            return;
//        }

        // Properly close all opened clients and the cluster
        cluster.close();

        //System.exit(0);
    }


    public static void addE(Vertex v_1, Vertex v_2, Edge edge) throws ExecutionException, InterruptedException {
      //g.V('thomas').addE('knows').to(g.V('mary'))

      String singleQuery = "g.V('"+ v_1.getId()+"').addE('"+edge.getLabel()+"')";
      if(edge.getId()!=null){
          singleQuery += ".property('id', '"+edge.getId()+"')";
      }
      for(Object key:edge.getProperties().keySet()){
          singleQuery += ".property('"+key+"', '"+edge.getProperties().get(key)+"')";
      }
      singleQuery += ".to(g.V('"+v_2.getId()+"'))";
      String gremlinQueries[] = new String[] {singleQuery};

      Cluster cluster;
      Client client;
      try {
          // Attempt to create the connection objects
          Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
          cluster = temp.create();
          client = cluster.connect();
      } catch (FileNotFoundException e) {
          // Handle file errors.
          System.out.println("Couldn't find the configuration file.");
          e.printStackTrace();
          return;
      }

      // After connection is successful, run all the queries against the server.
      for (String query : gremlinQueries) {
          System.out.println("\nSubmitting this Gremlin query: " + query);

          // Submitting remote query to the server.
          ResultSet results = client.submit(query);
          CompletableFuture<List<Result>> completableFutureResults;
          CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
          List<Result> resultList;
          Map<String, Object> statusAttributes;

          try{
              completableFutureResults = results.all();
              completableFutureStatusAttributes = results.statusAttributes();
              resultList = completableFutureResults.get();
              statusAttributes = completableFutureStatusAttributes.get();
          }
          catch(ExecutionException | InterruptedException e){
              e.printStackTrace();
              break;
          }
          catch(Exception e){
              ResponseException re = (ResponseException) e.getCause();

              // Response status codes. You can catch the 429 status code response and work on retry logic.
              System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
              System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));

              // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
              System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

              // Total Request Units (RUs) charged for the operation, upon failure.
              System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));

              // ActivityId for server-side debugging
              System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
              throw(e);
          }

          for (Result result : resultList) {
              System.out.println("\nQuery result:");
              System.out.println(result.toString());
          }

          // Status code for successful query. Usually HTTP 200.
          System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

          // Total Request Units (RUs) charged for the operation, after a successful run.
          System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
      }

      System.out.println("Demo complete!\n Press Enter key to continue...");
//      try{
//          System.in.read();
//      } catch (IOException e){
//          e.printStackTrace();
//          return;
//      }

      // Properly close all opened clients and the cluster
      cluster.close();

      //System.exit(0);
  }


    public static Vertex findVertexById(String id){
        Vertex vertex = new Vertex();
        String singleQuery = "g.V('"+id+"')";
        String gremlinQueries[] = new String[] {singleQuery};

        Cluster cluster;
        Client client;
        try {
            // Attempt to create the connection objects
            Cluster.Builder temp = Cluster.build(new File("src/remote.yaml"));
            cluster = temp.create();
            client = cluster.connect();
        } catch (FileNotFoundException e) {
            // Handle file errors.
            System.out.println("Couldn't find the configuration file.");
            e.printStackTrace();
            return null;
        }

        // After connection is successful, run all the queries against the server.
        for (String query : gremlinQueries) {
            System.out.println("\nSubmitting this Gremlin query: " + query);

            // Submitting remote query to the server.
            ResultSet results = client.submit(query);
            CompletableFuture<List<Result>> completableFutureResults;
            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
            List<Result> resultList;
            Map<String, Object> statusAttributes;

            try{
                completableFutureResults = results.all();
                completableFutureStatusAttributes = results.statusAttributes();
                resultList = completableFutureResults.get();
                statusAttributes = completableFutureStatusAttributes.get();
            }
            catch(ExecutionException | InterruptedException e){
                e.printStackTrace();
                break;
            }
            catch(Exception e){
                ResponseException re = (ResponseException) e.getCause();

                // Response status codes. You can catch the 429 status code response and work on retry logic.
                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code"));
                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code"));

                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

                // Total Request Units (RUs) charged for the operation, upon failure.
                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));

                // ActivityId for server-side debugging
                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
                throw(e);
            }

            for (Result result : resultList) {
                Map map_result = (Map)result.getObject();
                vertex.setId(map_result.get("id").toString());
                vertex.setLabel(map_result.get("label").toString());
//                for(){
//
//                }
                System.out.println("\nQuery result:");
                System.out.println(result.toString());
            }

            // Status code for successful query. Usually HTTP 200.
            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

            // Total Request Units (RUs) charged for the operation, after a successful run.
            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
        }

        System.out.println("Demo complete!\n Press Enter key to continue...");
//      try{
//          System.in.read();
//      } catch (IOException e){
//          e.printStackTrace();
//          return;
//      }

        // Properly close all opened clients and the cluster
        cluster.close();
        return vertex;
    }
}




