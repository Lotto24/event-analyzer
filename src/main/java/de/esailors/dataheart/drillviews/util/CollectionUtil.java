package de.esailors.dataheart.drillviews.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CollectionUtil {

	public static <T extends Comparable<? super T>> List<T> toSortedList(Collection<T> set) {
		List<T> sortedList = new ArrayList<>(set);
		Collections.sort(sortedList);
		return sortedList;
	}
	
}
