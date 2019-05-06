package com.hadoop.mr.order.topn;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

@Getter
@Setter
public class OrderBean implements WritableComparable<OrderBean>, Serializable {

	private String orderId;
	private String userId;
	private String pdtName;
	private float price;
	private int number;
	private float amounttFee;
	
	public void setAmounttFee(){
		this.amounttFee = this.price * this.number;
	}
	
	@Override
	public int compareTo(OrderBean o) {
		return o.getAmounttFee() > this.amounttFee ?1:0;
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(orderId);
		dataOutput.writeUTF(userId);
		dataOutput.writeUTF(pdtName);
		dataOutput.writeFloat(price);
		dataOutput.writeInt(number);
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.orderId = dataInput.readUTF();
		this.userId = dataInput.readUTF();
		this.pdtName = dataInput.readUTF();
		this.price = dataInput.readFloat();
		this.number = dataInput.readInt();
		this.amounttFee = this.price * this.number;
	}
	
	@Override
	public String toString() {
		return this.orderId +","+this.userId  +","+this.pdtName  +","+this.price  +","+this.number +","+this.amounttFee;
	}
}
