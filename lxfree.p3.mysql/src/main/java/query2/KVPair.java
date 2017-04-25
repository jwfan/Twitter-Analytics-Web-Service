package query2;

import java.util.Map.Entry;

public class KVPair implements Entry<Long, Integer> {

    private final Long key;
    private Integer value;
    
    public KVPair(Long key, Integer value) {
    	this.key = key;
    	this.value = value;
    }
	
	@Override
	public Long getKey() {
		return key;
	}

	@Override
	public Integer getValue() {
		return value;
	}

	@Override
	public Integer setValue(Integer value) {
		Integer old = this.value;
		this.value = value;
		return old;
	}

}
