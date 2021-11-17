package com.opstty;

// FARAMOND Emile - BENHALIMA Mustapha

// HADOOP
import org.apache.hadoop.util.ProgramDriver;

import com.opstty.districtsContainingTrees.DistrictsContainingTrees;
import com.opstty.sortTheTreesHeightFromSmallestToLargest.SortTheTreesHeightFromSmallestToLargest;
import com.opstty.numberOfTreesByKinds.NumberOfTreesByKinds;
import com.opstty.districtContainingTheMostTrees.DistrictContainingTheMostTrees;
import com.opstty.districtContainingTheOldestTree.DistrictContainingTheOldestTree;
import com.opstty.showAllExistingSpecies.ShowAllExistingSpecies;
import com.opstty.maximumHeightPerKindOfTree.MaximumHeightPerKindOfTree;

public class AppDriver {
    public AppDriver() {
    }

    public static void main(String[] argv) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();


        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A MapReduce job that counts the words in the input files.");
                //1.8.1 Districts containing trees /  DistrictsContainingTrees
            programDriver.addClass("districtsContainingTrees", DistrictsContainingTrees.class,
                    "A MapReduce job that counts the number of trees by rounding.");
                //1.8.2 Show all existing showAllExistingSpecies
            programDriver.addClass("showAllExistingSpecies", ShowAllExistingSpecies.class,
                    "A MapReduce job that display and show all existing species");
                //1.8.3 Number of trees by numberOfTreesByKinds   NumberOfTreesByKinds
            programDriver.addClass("numberOfTreesByKinds", NumberOfTreesByKinds.class,
                    "A MapReduce job that display the kind and number of trees of each kind");
                //1.8.4 Maximum height per kind of tree   / MaximumHeightPerKindOfTree
            programDriver.addClass("maximumHeightPerKindOfTree", MaximumHeightPerKindOfTree.class,
                    "A MapReduce job that list the highest tree by kind");
                //1.8.5 Sort the trees height from smallest to largest  / SortTheTreesHeightFromSmallestToLargest
            programDriver.addClass("sortTheTreesHeightFromSmallestToLargest", SortTheTreesHeightFromSmallestToLargest.class,
                    "A MapReduce job that sort by height ");
                //1.8.6 District containing the oldest tree
            programDriver.addClass("districtContainingTheOldestTree", DistrictContainingTheOldestTree.class,
                     "A MapReduce job that displays the district where the oldest tree");
                //1.8.7 District containing the most trees
            programDriver.addClass("districtContainingTheMostTrees", DistrictContainingTheMostTrees.class,
                     "A MapReduce job that displays the district that contains the most trees");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
