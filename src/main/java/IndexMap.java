
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IndexMap {
	public Map<String, ArrayList<String>> hashMap;

	public IndexMap() {
		hashMap = new HashMap<String, ArrayList<String>>();
	}
	
	public void addValues(String key, String value) {
	   ArrayList<String> tempList = null;
	   if (hashMap.containsKey(key)) {
	      tempList = hashMap.get(key);
	      if(tempList == null) {
	         tempList = new ArrayList<String>();
	      }
	      tempList.add(value);  
	   } else {
	      tempList = new ArrayList<String>();
	      tempList.add(value);               
	   }
	   hashMap.put(key, tempList);
	}
	
	public ArrayList<String> getValues(String key) {
		ArrayList<String> result = hashMap.get(key);
		return result;
	}
}

