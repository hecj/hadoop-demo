package com.hadoop.mr.flow;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 上行和下行流量模型类
 * 本案例的功能:演示自定义数据类型如何实现hadoop的序列化接口
 * 1.该类一定要保留空参构造函数
 * 2.write方法中输出字段二进制数据的顺序要与readFields方法读取数据的顺序一致
 */
@Getter
@Setter
public class FlowBean implements Writable {
	
	private int upFlow;
	private int dFlow;
	private int amountFlow;
	private String phone;
	
	public FlowBean(){
	
	}
	
	public FlowBean(String phone,int upFlow,int dFlow){
		this.phone = phone;
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.amountFlow = upFlow + dFlow;
	}
	
	/**
	 * hadoop系统在序列化该类的对象时要调用的方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upFlow);
		out.writeUTF(phone);
		out.writeInt(dFlow);
		out.writeInt(amountFlow);
	}
	
	/**
	 * hadoop系统在序反列化该类的对象时要调用的方法
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readInt();
		this.phone = in.readUTF();
		this.dFlow = in.readInt();
		this.amountFlow = in.readInt();
	}
	
	@Override
	public String toString() {
		return this.phone + ","+this.upFlow +","+ this.dFlow +"," + this.amountFlow;
	}
}
