package de.esailors.dataheart.drillviews.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CollectionUtil {

	public static <T extends Comparable<? super T>> List<T> toSortedList(Set<T> set) {
		List<T> sortedList = new ArrayList<>(set);
		Collections.sort(sortedList);
		return sortedList;
	}
	
}
