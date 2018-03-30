package org.apache.avro.util.test;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


public class AvroUnit
{
    public static void assertEquals(File actual, File expected) throws IOException
    {
        assertEquals(actual, expected, true);
    }

    public static void assertNotEquals(File actual, File expected) throws IOException
    {
        assertEquals(actual, expected, false);
    }

    private static void assertEquals(File actual, File expected, boolean equality) throws IOException
    {
        DatumReader<GenericRecord> aDatumReader = new GenericDatumReader<>();
        DatumReader<GenericRecord> eDatumReader = new GenericDatumReader<>();
        try (
             DataFileReader<GenericRecord> aDataFileReader = new DataFileReader<>(actual, aDatumReader);
             DataFileReader<GenericRecord> eDataFileReader = new DataFileReader<>(expected, eDatumReader);
            )
        {
            Schema aSchema = aDataFileReader.getSchema();
            Schema eSchema = eDataFileReader.getSchema();
            assertEqualsSchema(aSchema, eSchema, equality);

            while (aDataFileReader.hasNext() && eDataFileReader.hasNext())
            {
                GenericRecord aRecord = aDataFileReader.next();
                GenericRecord eRecord = eDataFileReader.next();

                assertRecordsEqual(aRecord, eRecord, equality);
            }
        }
    }

    private static void assertRecordsEqual(GenericRecord aRecord, GenericRecord eRecord, boolean equality)
    {
        GenericData.Record ar = (GenericData.Record) aRecord;
        GenericData.Record er = (GenericData.Record) eRecord;

        int cmp = ar.compareTo(er, true);

        String message = null;
        if (cmp != 0 && equality) {
            message = "Record " + ar + " is different from expected record " + er + ". Comparison code: " + cmp + ".";
        }
        else if (cmp == 0 && !equality)
            message = "Record " + ar + " is equal to expected record " + er + ", which is unexpected";

        if (message !=null)
            throw new AssertionError(message);
    }

    private static void assertEqualsSchema(Schema actual, Schema expected, boolean equality)
    {
        String message = null;
        boolean schemaEquals = actual.equals(expected);

        if (!schemaEquals && equality)
            message = "Actual schema doesn't match the expected one.";
        else if (schemaEquals && !equality)
            message = "Actual schema is not different the expected one.";

        if (message != null)
            throw new AssertionError(message);
    }
}
