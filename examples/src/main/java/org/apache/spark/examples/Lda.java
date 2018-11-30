package org.apache.spark.examples;

import cc.mallet.types.Dirichlet;
import cc.mallet.util.Randoms;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.Vector;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class Lda {

    static java.util.Random rd = new java.util.Random(5L);

    /**
     * Parses numbers split by whitespace to a Tuple2<Integer, Vector>
     */
    static Tuple2<Integer, Vector> parseVector(String line) {
        String[] splits = line.split(" ");
        //parse the doc_id.
        int doc_id = Integer.parseInt(splits[0]);
        //parse the document word_count pair.
        double[] data = new double[splits.length - 1];
        for (int i = 0; i < data.length; i++)
            data[i] = Double.parseDouble(splits[i + 1]);

        return new Tuple2<Integer, Vector>(doc_id, new Vector(data));
    }

    static Vector initialize_doc_topic_prob(Integer topic_size) {
        double alpha[] = new double[topic_size.intValue()];
        Arrays.fill(alpha, 1);
        Dirichlet dirichlet = new Dirichlet(alpha);
        double[] probability = dirichlet.nextDistribution();
        return new Vector(probability);
    }

    //return [<topic_id, word_id>]
    static Vector sample_doc_word_topic(Tuple2<Vector, Vector> doc_wordcount_docProb) {
        Randoms random = new Randoms(rd.nextInt());
//        random.setSeed((long) (Math.random() * Long.MAX_VALUE));

        ArrayList<Double> resultList = new ArrayList<Double>();

        Vector wordCountVec = doc_wordcount_docProb._1();
        Vector topicProbVec = doc_wordcount_docProb._2();
        double[] wordCount = wordCountVec.elements();
        double[] topicProb = topicProbVec.elements();

        int word_id, count;
        for (int i = 0; i < wordCount.length; i += 2) {
            word_id = (int) (wordCount[i]);
            count = (int) (wordCount[i + 1]);

            for (int j = 0; j < count; j++) {
                //all the pairs are recorded as (topic_id, word_id) in the resultList
                int selected = random.nextDiscrete(topicProb);
                resultList.add((double) selected);
                resultList.add((double) word_id);
            }
        }

        double[] resultArray = new double[resultList.size()];
        for (int k = 0; k < resultList.size(); k++) {
            resultArray[k] = resultList.get(k);
        }

        return new Vector(resultArray);
    }

    //return [<topic_id, [word_id]>]
    static Iterable<Tuple2<Integer, Vector>> transpose_topic_word_element(Tuple2<Integer, Vector> topic_word_element) {
        HashMap<Integer, ArrayList<Integer>> resultMap = new HashMap<Integer, ArrayList<Integer>>();

        int doc_id = topic_word_element._1();
        Vector topic_word_Vec = topic_word_element._2();
        double[] topic_word_array = topic_word_Vec.elements();

        int topic_id, word_id;
        for (int i = 0; i < topic_word_array.length; i += 2) {
            topic_id = (int) (topic_word_array[i]);
            word_id = (int) (topic_word_array[i + 1]);

            if (resultMap.containsKey(topic_id)) {
                resultMap.get(topic_id).add(word_id);
            } else {
                ArrayList<Integer> tempList = new ArrayList<Integer>();
                tempList.add(word_id);
                resultMap.put(topic_id, tempList);
            }
        }

        ArrayList<Tuple2<Integer, Vector>> resultList = new ArrayList<Tuple2<Integer, Vector>>();
        for (Integer key : resultMap.keySet()) {
            ArrayList<Integer> list = resultMap.get(key);
            double[] words = new double[list.size()];
            for (int k = 0; k < list.size(); k++) {
                words[k] = list.get(k);
            }

            resultList.add(new Tuple2<Integer, Vector>(key, new Vector(words)));
        }

        return resultList;
    }

    static Vector addWordCount(Vector vector1, Vector vector2, int dic_size) {
        // If the first element is -1, that means the vector is an intermediate result.
        double[] values1 = vector1.elements();
        double[] values2 = vector2.elements();

        if (values1[0] == -1 && values2[0] == -1) {
            for (int i = 1; i < values2.length; i++) {
                values1[i] = values1[i] + values2[i];
            }

            return new Vector(values1);
        } else if (values1[0] == -1 && values2[0] != -1) {
            for (int i = 0; i < values2.length; i++) {
                //a word's index is word_id + 1. The first is used to save the token "-1".
                values1[(int) values2[i] + 1]++;
            }

            return new Vector(values1);
        } else if (values1[0] != -1 && values2[0] == -1) {
            for (int i = 0; i < values1.length; i++) {
                values2[(int) values1[i] + 1]++;
            }

            return new Vector(values2);
        } else {
            double[] result = new double[dic_size];
            Arrays.fill(result, 0.0);
            result[0] = -1;

            for (int i = 0; i < values2.length; i++) {
                result[(int) values2[i] + 1]++;
            }

            for (int i = 0; i < values1.length; i++) {
                result[(int) values1[i] + 1]++;
            }

            return new Vector(result);
        }
    }

    static Vector sample_topic_word_prob(Vector word_count, int dic_size) {
        double[] word_count_array = new double[dic_size];
        Arrays.fill(word_count_array, 1.0);

        double[] word_c = word_count.elements();

        if (word_c[0] == -1)//aggregate result
        {
            for (int i = 1; i < word_c.length; i++) {
                word_count_array[i - 1] += word_c[i];
            }
        } else {
            for (int i = 0; i < word_c.length; i++) {
                word_count_array[(int) word_c[i]]++;
            }
        }

        Dirichlet dirichlet = new Dirichlet(word_count_array);
        return new Vector(dirichlet.nextDistribution());
    }

    static Vector default_topic_word_prob(int dic_size) {
        double[] word_c = new double[dic_size];
        Arrays.fill(word_c, 1);

        Dirichlet dirichlet = new Dirichlet(word_c);
        return new Vector(dirichlet.nextDistribution());
    }


    //topic_word has the form [<topic_id, word_id>].
    static Vector update_doc_word_topic(Vector topic_word, double[][] topic_word_prob) {
        Randoms random = new Randoms(rd.nextInt());
//        random.setSeed((long) (Math.random() * Long.MAX_VALUE));

        //1. sample the topic distribution for the document.
        double topic_word_array[] = topic_word.elements();
        double topic_count[] = new double[topic_word_prob.length];
        Arrays.fill(topic_count, 1);
        for (int i = 0; i < topic_word_array.length; i += 2) {
            topic_count[(int) topic_word_array[i]]++;
        }

        Dirichlet dirichlet = new Dirichlet(topic_count);
        double doc_topic[] = dirichlet.nextDistribution();

        int word_id;
        double sum;
        double[] temp_topic_array = new double[topic_word_prob.length];
        for (int i = 0; i < topic_word_array.length; i += 2) {
            word_id = (int) topic_word_array[i + 1];

            System.arraycopy(doc_topic, 0, temp_topic_array, 0, doc_topic.length);
            sum = 0;
            for (int j = 0; j < temp_topic_array.length; j++) {
                temp_topic_array[j] *= topic_word_prob[j][word_id];
                sum += temp_topic_array[j];
            }

            for (int j = 0; j < temp_topic_array.length; j++) {
                temp_topic_array[j] = temp_topic_array[j] / sum;
            }
            topic_word_array[i] = random.nextDiscrete(temp_topic_array);
        }

        return new Vector(topic_word_array);
    }

    //return [<topic_id, doc_id>]
    static Tuple2<Integer, Integer> transpose_topic_doc(Tuple2<Integer, Vector> topic_word_element) {
        HashMap<Integer, Integer> resultMap = new HashMap<Integer, Integer>();

        int doc_id = topic_word_element._1();
        Vector topic_word_Vec = topic_word_element._2();
        double[] topic_word_array = topic_word_Vec.elements();
        int finalTopic = -1;

        int topic_id, word_id;
        for (int i = 0; i < topic_word_array.length; i += 2) {
            topic_id = (int) (topic_word_array[i]);
            word_id = (int) (topic_word_array[i + 1]);

            if (resultMap.containsKey(topic_id)) {
                resultMap.put(topic_id, resultMap.get(topic_id) + 1);
            } else {
                resultMap.put(topic_id, 1);
            }
        }

        int maxValueInMap = (Collections.max(resultMap.values()));
        for (Map.Entry<Integer, Integer> entry : resultMap.entrySet()) {  // Itrate through hashmap
            if (entry.getValue() == maxValueInMap) {
                finalTopic = entry.getKey();
                break;
                //	System.out.println(entry.getKey());     // Print the key with max value
            }
        }

        return new Tuple2<Integer, Integer>(finalTopic, doc_id);
    }


    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 4) {
            System.err.println("Usage: JavaLDA <GMM dataset file> <dic_size> <topic_size> <max_superstep>");
            System.exit(1);
        }
        long startTime = new Date().getTime();
        JavaSparkContext sc = new JavaSparkContext();
        //spark://master:7077
        //System.setProperty("spark.eventLog.enabled", "true");
//        System.setProperty("spark.broadcast.compress", "false");
//        System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast." + args[5] + "BroadcastFactory");
//        System.setProperty("spark.broadcast.blockSize", "64000000");
//        JavaSparkContext sc = new JavaSparkContext(args[0], "JavaLDA");
//        JavaSparkContext sc = new JavaSparkContext(args[0], "JavaLDA",
//			"/usr/lib/spark-0.8.0-incubating-bin-cdh4", "/usr/lib/spark-0.8.0-incubating-bin-cdh4/examples/target/spark-examples_2.9.3-0.8.0-incubating.jar");
        //	"/usr/lib/spark-0.8.0-incubating-bin-cdh4", System.getenv("SPARK_EXAMPLES_JAR"));
//                "/home/yx15/spark-1.5.1", System.getenv("SPARK_EXAMPLES_JAR"));
//		sc.addJar("/usr/lib/spark-0.8.0-incubating-bin-cdh4/plug-in/commons-math3-3.1.1-sources.jar");
        //sc.addJar("/usr/lib/spark-0.8.0-incubating-bin-cdh4/examples/lib/commons-math3-3.0.jar");
//		sc.addJar("/usr/lib/spark-0.8.0-incubating-bin-cdh4/plug-in/mallet-2.0.7-sources.jar");	
        //sc.addJar("/usr/lib/spark-0.8.0-incubating-bin-cdh4/examples/lib/mallet.jar");
        //	String path = "hdfs://master:54310" + args[1];

        String path = "file:" + args[0];
        final int dic_size = Integer.parseInt(args[1]);
        final int topic_size = Integer.parseInt(args[2]);
        int max_superstep = Integer.parseInt(args[3]);
        System.gc();

        //1. read the data set. Vector has the form [<word_id, count>].
        JavaPairRDD<Integer, Vector> doc_word_count = sc.textFile(path).mapToPair(
                new PairFunction<String, Integer, Vector>() {
                    @Override
                    public Tuple2<Integer, Vector> call(String line) throws Exception {
                        return parseVector(line);
                    }
                }
                //).persist(StorageLevels.DISK_ONLY);
        ).persist(StorageLevels.MEMORY_ONLY);

        final long doc_count = doc_word_count.count();
        System.out.println("doc_count = " + doc_count + ", topic_size = " + topic_size + ", dic_size = " + dic_size + ", max_superstep = " + max_superstep);

        if (doc_count <= 1) {
            System.err.println("doc_count is less than 2");
            System.exit(1);
        }

        //2. initialize the topic probability for the documents.
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < doc_count; i++) {
            list.add(i);
        }

        JavaPairRDD<Integer, Vector> doc_topic_probability = sc.parallelize(list).mapToPair(
                new PairFunction<Integer, Integer, Vector>() {
                    @Override
                    public Tuple2<Integer, Vector> call(Integer doc_id) throws Exception {
                        return new Tuple2<Integer, Vector>(doc_id, initialize_doc_topic_prob(new Integer(topic_size)));
                    }
                }
        );

        //3. initialize the topic assignment of the words in each documents: returns [<topic_id, word_id>]
        JavaPairRDD<Integer, Vector> doc_topic_word = doc_word_count.join(doc_topic_probability).mapValues(
                new Function<Tuple2<Vector, Vector>, Vector>() {
                    public Vector call(Tuple2<Vector, Vector> wordcount_docProb) throws Exception {
                        return sample_doc_word_topic(wordcount_docProb);
                    }
                    //}).persist(StorageLevels.DISK_ONLY);
                }
        ).persist(StorageLevels.MEMORY_ONLY);
        //		}).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY);

        //4. MCMC loop
        JavaPairRDD<Integer, Vector> topic_word_count_map;
        JavaPairRDD<Integer, Vector> topic_word_prob_map;
        Map<Integer, Vector> collected_topic_word_prob;
//		Map<Integer, Vector> topic_word_prob;

        long initTime = new Date().getTime();
        System.out.println("baseline time: " + ((initTime - startTime) / 1000) + " seconds");
        try {
            //      PrintWriter writer1=new PrintWriter(new FileOutputStream(new File("java_output.txt"), true));
            PrintWriter writer1 = new PrintWriter("java_output.txt");
            writer1.println("baseline time: " + ((initTime - startTime) / 1000) + " seconds");
            writer1.close();
        } catch (IOException e) {
            System.out.println("File writing failure");
        }

        for (int loop = 0; loop < max_superstep; loop++) {
            System.out.println("**************** iteration #" + loop + " ****************");
            long time1 = new Date().getTime();
            //4.1 Compute the word count for each topic
            JavaPairRDD<Integer, Vector> topic_word_count = doc_topic_word.flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Integer, Vector>, Integer, Vector>() {
                        public Iterable<Tuple2<Integer, Vector>> call(Tuple2<Integer, Vector> topic_word_element) throws Exception {
                            return transpose_topic_word_element(topic_word_element);
                        }
                    }
            );

            topic_word_count_map = topic_word_count.reduceByKey(
                    new Function2<Vector, Vector, Vector>() //<data, data> => new model of this
                    {
                        @Override
                        public Vector call(Vector data1, Vector data2) {
                            return addWordCount(data1, data2, dic_size);
                        }
                    }
            );

            //4.2 Sample the topic_word distribution
            topic_word_prob_map = topic_word_count_map.mapValues(
                    new Function<Vector, Vector>() {
                        @Override
                        public Vector call(Vector word_count) {
                            return sample_topic_word_prob(word_count, dic_size);
                        }
                    }
            );

            collected_topic_word_prob = topic_word_prob_map.collectAsMap();
            if (collected_topic_word_prob.size() < topic_size) {
                for (int i = 0; i < topic_size; i++) {
                    if (!collected_topic_word_prob.containsKey(i)) {
                        collected_topic_word_prob.put(i, default_topic_word_prob(dic_size));
                    }
                }
            }

            final double[][] topic_word_prob = new double[topic_size][];
            for (int i = 0; i < topic_size; i++) {
                topic_word_prob[i] = collected_topic_word_prob.get(i).elements();
            }

            if (loop < max_superstep) {
                //System.out.println("Broadcasting topic_word_prob matrix...");
                final Broadcast<double[][]> broadcastVar = sc.broadcast(topic_word_prob);

                //Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});
                //broadcastVar.value();

                        /*System.out.println("&&&&& topic_word_prob &&&&&");
                        for (int k = 0; k < topic_word_prob.length; k++) {
                            double[] row = topic_word_prob[k];
                            for (int t = 0; t < row.length; t++) {
                                System.out.print(row[t] + " ");
                            }
                            System.out.println();
                        }*/

                //4.3 sample the doc_topic_distribution
                doc_topic_word = doc_topic_word.mapValues(
                        new Function<Vector, Vector>() {
                            @Override
                            public Vector call(Vector topic_word) {
                                //return update_doc_word_topic(topic_word, topic_word_prob);
                                return update_doc_word_topic(topic_word, broadcastVar.value());
                            }
                        }
                        //).persist(StorageLevels.DISK_ONLY);
                ).persist(StorageLevels.MEMORY_ONLY);
                //	).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY);

                long endTime = new Date().getTime();
                System.out.println("loop " + loop + " time: " + ((endTime - time1) / 1000) + " seconds");
                try {
                    PrintWriter writer2 = new PrintWriter(new FileOutputStream(new File("java_output.txt"), true));
                    writer2.println("loop " + loop + " time: " + ((endTime - time1) / 1000) + " seconds");
                    //Iterator<Tuple2<Integer,Vector>> iter = doc_topic_word.toLocalIterator();
                    //while (iter.hasNext()) {
                    //	Tuple2<Integer, Vector> element = iter.next();
                    //	int key = element._1;
                    //	Vector value = element._2;
                    //	writer2.println("Key: "+key);
                    //	for (int i = 0; i < value.numActives(); i++) {
                    //		writer2.print(value.apply(i)+ " ");
                    //	}
                    //	writer2.println();
                    //}

                    //for (int k = 0; k < topic_word_prob.length; k++) {
                    //	double[] row = topic_word_prob[k];
                    //	for (int t = 0; t < row.length; t++) {
                    //		writer2.print(row[t] + " ");
                    //	}
                    //	writer2.println();
                    //}
                    //writer2.println();
                    writer2.close();
                } catch (IOException e) {
                    System.out.println("File writing failure");
                }
            }
        } // end of loop

		/*Map<Integer, Vector> finalResultMap = doc_topic_word.collectAsMap();
        Iterator<Entry<Integer, Vector>> iterator = finalResultMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, Vector> entry = (Map.Entry<Integer, Vector>) iterator.next();
			System.out.println("Key : " + entry.getKey() + " Value :");
			double[] tmp = entry.getValue().elements();
			for (int i = 0; i < tmp.length; i++) {
				System.out.print(tmp[i] + " ");
			}
			System.out.println();
		}*/		


		/*
        JavaPairRDD<Integer, Integer> topic_doc = doc_topic_word.map(
                	new PairFunction<Tuple2<Integer, Vector>, Integer, Integer>(){
                        	public Tuple2<Integer, Integer> call(Tuple2<Integer, Vector> topic_word_element) throws Exception {
                                	return transpose_topic_doc(topic_word_element);
                                }
                });

		JavaPairRDD<Integer, List<Integer>> topic_doc_array = topic_doc.groupByKey();
		List<Tuple2<Integer, List<Integer>>> topic_doc_list = topic_doc_array.collect();
		System.out.println("Result: [topic, [doc1, doc2, ...]]");
		System.out.println(topic_doc_list.toString());		
		*/
    }
}
