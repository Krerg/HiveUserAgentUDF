package com.mylnikov;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class UserAgentUDTFTest {

    private UserAgentUDTF userAgentUDTF = new UserAgentUDTF();

    @Test
    public void shouldProperlyInitialize() throws UDFArgumentException {
        List<String> fieldNames = Collections.singletonList("field1");
        List<ObjectInspector> fieldsOIs = new ArrayList<>(1);
        fieldsOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        StructObjectInspector result = userAgentUDTF.initialize(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsOIs));
        Assert.assertEquals(3, result.getAllStructFieldRefs().size());
    }

    @Test(expected = UDFArgumentException.class)
    public void shouldThrowExcpetionInCaseInvalidConfig() throws UDFArgumentException {
        List<String> fieldNames = Arrays.asList("field1", "field2");
        List<ObjectInspector> fieldsOIs = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            fieldsOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        userAgentUDTF.initialize(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsOIs));
    }

    @Test
    public void shouldParseUserAgent() throws HiveException {
        shouldProperlyInitialize();
        TestCollector testCollector = new TestCollector();
        userAgentUDTF.setCollector(testCollector);
        userAgentUDTF.process(new Object[] {"Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0"});
        Object[] outputValue = (Object[]) testCollector.getInput();
        Assert.assertEquals(3, outputValue.length);
        Assert.assertEquals("Computer", outputValue[0]);
        Assert.assertEquals("Mac OS X", outputValue[1]);
        Assert.assertEquals("Firefox 42", outputValue[2]);
    }
}

class TestCollector implements Collector {

    private Object input = new Object();

    public Object getInput() {
        return input;
    }

    @Override
    public void collect(Object input) throws HiveException {
        this.input = input;
    }
}
