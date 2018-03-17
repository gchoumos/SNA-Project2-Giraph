/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.examples.SimpleBoggleComputation.TextArrayListWritable;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.Scanner;

/**
 * Demonstrates the basic Pregel dispersion implementation.
 */
@Algorithm(
    name = "Boggle",
    description = "Sets the vertex value of each vertex to a list of boggle words ending in that vertex"
)
public class SimpleBoggleComputation extends BasicComputation<
    Text, TextArrayListWritable, NullWritable, TextArrayListWritable> {

	
	//private static final TreeSet<String> dictionary = new TreeSet<String>(Arrays.asList("GEEKS", "SOCIAL", "NETWORK", "ANALYSIS", "QUIZ"));
	// Read from the file!

	private static final TreeSet<String> dictionary = getDictionaryFromFile("dictionary_bonus.txt");// new TreeSet<String>();

	/** Class logger */
	  private static final Logger LOG =
	      Logger.getLogger(SimpleBoggleComputation.class);

	  @Override
	  public void compute(
	      Vertex<Text, TextArrayListWritable, NullWritable> vertex,
	      Iterable<TextArrayListWritable> messages) throws IOException {

		  if(getSuperstep() == 0){
			  TextArrayListWritable aw = new TextArrayListWritable();
			  aw.add(new Text(vertex.getId().toString().substring(0,1)));
			  aw.add(vertex.getId());

			  sendMessageToAllEdges(vertex, aw);
		  } else {//if (getSuperstep() < 100) {
			  System.out.println("Step: " + getSuperstep() + " Vertex Id: " + vertex.getId());
			  
			  // Get the new letter that will be appended
			  String new_letter = vertex.getId().toString().substring(0,1);

			  for (TextArrayListWritable x:messages){
				  System.out.println("Vertex id: " + vertex.getId() + " Message: " + x + " We want to append: " + new_letter);
				  
				  // The new word that occurs by appending the current letter to the message
				  Text new_word = new Text(x.get(0) + new_letter);
				  
				  // Add the current vertex id in the message. This will be checked later in order not to revisit a vertex.
				  x.add(vertex.getId());
				  
				  // Does the new word already exist in the dictionary?
				  if(dictionary.contains(new_word.toString())){
					  // It exists, so add it to the vertex value. If it already exists then I consider it ok to re-add it
					  // as it means that the same word can be formed by following different paths which end to this node.
					  // This is a detail which seems more like a matter of implementation.
					  System.out.println("Found a matching word in the dicitionary! --> " + new_word);
					  vertex.getValue().add(new_word);
				  }
				  
				  // We want to create a list of edges that are OK to be visited. It will happen with 2 checks.
				  // ------------------------------------------------------------------------------------------
				  // 1. Make sure that the target vertex has not already been visited
				  // 2. Make sure that there exist words with the one that was created now as a prefix (otherwise it would be pointless).
				  
				  // Create the list of edges to send the message to. Initially empty
				  TextArrayListWritable edgesToUse = new TextArrayListWritable();
				  
				  // Is the new word a prefix of any word in the dictionary?? Note that we don't want this to hold true
				  // because of the word itself. The word itself has been already checked. So we'll check for words with
				  // at least one more character.
				  if(!dictionary.subSet(new_word.toString() + Character.MIN_VALUE,new_word.toString() + Character.MAX_VALUE).isEmpty()){
					  System.out.println("There exists at least one word which has " + new_word + " as a prefix");
					  // Iterate through the edges to see if they have been already visited. If they have, we won't send messages to them.
					  for (Edge<Text, NullWritable> y:vertex.getEdges()){
						  // If the target vertex is already in the message, then it was already visited
						  if(x.contains(y.getTargetVertexId())){
							  System.out.println("Excluding " + y.getTargetVertexId() + " as it has been visited already.");
						  } else {
							  // Has not been visited so we add it to the list of vertices that the message will be sent to
							  edgesToUse.add(new Text(y.getTargetVertexId()));
							  System.out.println("Target: " + y.getTargetVertexId() + " has not yet been visited. So it'll get the msg");
							  //System.out.println("Current state of edgesToUse: " + edgesToUse);
						  }
					  }
				  } else {
					  System.out.println("No words exist that have " + new_word + " as a prefix.");
					  // So no further action required here, just continue with the loop on the messages
					  continue;
				  }
				  
				  // Change the old word with the new one that was created after appending the character of this vertex
				  x.set(0,new_word);
				  System.out.println("Vertex id: " + vertex.getId() + " Edges to use: " + edgesToUse);
				  // Send the message to the vertices that are eligible to receive it. No pointless communication will take place!
				  sendMessageToMultipleEdges(edgesToUse.iterator(),x);
			  }
		  }/* else {
			  //print the vertex values just to see what happened
			  System.out.println("Vertex id: " + vertex.getId() + " Values: " + vertex.getValue());
			  //vertex.voteToHalt();
		  }*/
		  vertex.voteToHalt();
  }
  
  // I'll use this function in order to populate the dictionary from a file
  public static TreeSet getDictionaryFromFile(String fname) {
	  TreeSet<String> words_from_file = new TreeSet<String>();
	  Scanner sc = null;
	    try {
	        sc = new Scanner(new File(fname));
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();  
	    }
	    while (sc.hasNextLine()) {
	        String word = new String(sc.nextLine().toUpperCase());
	    	//System.out.print(word);
	        words_from_file.add(word);
	    }
	  System.out.println("The size of the word set is: " + words_from_file.size());
	  return words_from_file;
  }
  /** Utility class for delivering the array of vertices THIS vertex
    * should connect with to close triangles with neighbors */
  public static class TextArrayListWritable
    extends ArrayListWritable<Text> {
	private static final long serialVersionUID = -7220517688447798587L;
	/** Default constructor for reflection */
    public TextArrayListWritable() {
      super();
    }
    
    /** Set storage type for this ArrayListWritable */
    @Override
    @SuppressWarnings("unchecked")
    public void setClass() {
      setClass(Text.class);
    }
  }
}