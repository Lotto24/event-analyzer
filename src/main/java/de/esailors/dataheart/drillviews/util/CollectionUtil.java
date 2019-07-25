package de.esailors.dataheart.drillviews.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class CollectionUtil {

	public static <T extends Comparable<? super T>> List<T> toSortedList(Collection<T> set) {
		List<T> sortedList = new ArrayList<>(set);
		Collections.sort(sortedList);
		return sortedList;
	}

	public static <T> T popFromSet(Set<T> set) {
		if(set == null || set.isEmpty()) {
			throw new IllegalArgumentException("Expect given set to be neither null nor empty");
		}
		for(T t : set) {
			return t;
		}
		throw new IllegalStateException("Must not be reachable");
	}
	
}
