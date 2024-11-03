package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductRating implements org.apache.hadoop.io.Writable{
    private String productId;
    private double rating;

    public ProductRating(String productId, double rating) {
        this.productId = new String(productId);
        this.rating = rating;
    }

    //SE NON METTI IL COSTRUTTORE VUOTO NON FUNZIONA IL JOB
    public ProductRating() {
	}

    public String getProductId() {
        return productId;
    }

    public double getRating() {
        return rating;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public void readFields(DataInput in) throws IOException {
		rating = in.readDouble();
		productId = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(rating);
		out.writeUTF(productId);
	}
}
