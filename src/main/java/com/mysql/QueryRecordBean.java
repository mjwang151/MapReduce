package com.mysql;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @ClassName QueryRecordBean
 * @Author mjwang
 * @Date 2021/5/11 17:30
 * @Description QueryRecordBean
 * @Version 1.0
 */
public class QueryRecordBean implements DBWritable, WritableComparable<QueryRecordBean> {
    private String QueryId;
    private String TransCode;
    private String TransInfo;
    private String QueryAccount;
    private String WSStatus;
    private String WSDetail;
    private String ErrMsg;
    private String DataSource;
    private String relaDSTrans;
    private String retDataNum;
    private String RequestAddr;
    private String Begintime;
    private String Endtime;
    private String transkeyword;
    private String traceId;

    public String getQueryId() {
        return QueryId;
    }

    public void setQueryId(String queryId) {
        QueryId = queryId;
    }

    public String getTransCode() {
        return TransCode;
    }

    public void setTransCode(String transCode) {
        TransCode = transCode;
    }

    public String getTransInfo() {
        return TransInfo;
    }

    public void setTransInfo(String transInfo) {
        TransInfo = transInfo;
    }

    public String getQueryAccount() {
        return QueryAccount;
    }

    public void setQueryAccount(String queryAccount) {
        QueryAccount = queryAccount;
    }

    public String getWSStatus() {
        return WSStatus;
    }

    public void setWSStatus(String WSStatus) {
        this.WSStatus = WSStatus;
    }

    public String getWSDetail() {
        return WSDetail;
    }

    public void setWSDetail(String WSDetail) {
        this.WSDetail = WSDetail;
    }

    public String getErrMsg() {
        return ErrMsg;
    }

    public void setErrMsg(String errMsg) {
        ErrMsg = errMsg;
    }

    public String getDataSource() {
        return DataSource;
    }

    public void setDataSource(String dataSource) {
        DataSource = dataSource;
    }

    public String getRelaDSTrans() {
        return relaDSTrans;
    }

    public void setRelaDSTrans(String relaDSTrans) {
        this.relaDSTrans = relaDSTrans;
    }

    public String getRetDataNum() {
        return retDataNum;
    }

    public void setRetDataNum(String retDataNum) {
        this.retDataNum = retDataNum;
    }

    public String getRequestAddr() {
        return RequestAddr;
    }

    public void setRequestAddr(String requestAddr) {
        RequestAddr = requestAddr;
    }

    public String getBegintime() {
        return Begintime;
    }

    public void setBegintime(String begintime) {
        Begintime = begintime;
    }

    public String getEndtime() {
        return Endtime;
    }

    public void setEndtime(String endtime) {
        Endtime = endtime;
    }

    public String getTranskeyword() {
        return transkeyword;
    }

    public void setTranskeyword(String transkeyword) {
        this.transkeyword = transkeyword;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(QueryId);
        dataOutput.writeUTF(TransCode);
        dataOutput.writeUTF(TransInfo);
        dataOutput.writeUTF(QueryAccount);
        dataOutput.writeUTF(WSStatus);
        dataOutput.writeUTF(WSDetail);
        dataOutput.writeUTF(ErrMsg);
        dataOutput.writeUTF(DataSource);
        dataOutput.writeUTF(relaDSTrans);
        dataOutput.writeUTF(retDataNum);
        dataOutput.writeUTF(RequestAddr);
        dataOutput.writeUTF(Begintime);
        dataOutput.writeUTF(Endtime);
        dataOutput.writeUTF(transkeyword);
        dataOutput.writeUTF(traceId);


    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.QueryId = dataInput.readUTF();
        this.TransCode = dataInput.readUTF();
        this.TransInfo = dataInput.readUTF();
        this.QueryAccount = dataInput.readUTF();
        this.WSStatus = dataInput.readUTF();
        this.WSDetail = dataInput.readUTF();
        this.ErrMsg = dataInput.readUTF();
        this.DataSource = dataInput.readUTF();
        this.relaDSTrans = dataInput.readUTF();
        this.retDataNum = dataInput.readUTF();
        this.RequestAddr = dataInput.readUTF();
        this.Begintime = dataInput.readUTF();
        this.Endtime = dataInput.readUTF();
        this.transkeyword = dataInput.readUTF();
        this.traceId = dataInput.readUTF();

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.QueryId);
        preparedStatement.setString(2, this.TransCode);
        preparedStatement.setString(3, this.TransInfo);
        preparedStatement.setString(4, this.QueryAccount);
        preparedStatement.setString(5, this.WSStatus);
        preparedStatement.setString(6, this.WSDetail);
        preparedStatement.setString(7, this.ErrMsg);
        preparedStatement.setString(8, this.DataSource);
        preparedStatement.setString(9, this.relaDSTrans);
        preparedStatement.setString(10, this.retDataNum);
        preparedStatement.setString(11, this.RequestAddr);
        preparedStatement.setString(12, this.Begintime);
        preparedStatement.setString(13, this.Endtime);
        preparedStatement.setString(14, this.transkeyword);
        preparedStatement.setString(15, this.traceId);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.QueryId = resultSet.getString(1);
        this.TransCode = resultSet.getString(2);
        this.TransInfo = resultSet.getString(3);
        this.QueryAccount = resultSet.getString(4);
        this.WSStatus = resultSet.getString(5);
        this.WSDetail = resultSet.getString(6);
        this.ErrMsg = resultSet.getString(7);
        this.DataSource = resultSet.getString(8);
        this.relaDSTrans = resultSet.getString(9);
        this.retDataNum = resultSet.getString(10);
        this.RequestAddr = resultSet.getString(11);
        this.Begintime = resultSet.getString(12);
        this.Endtime = resultSet.getString(13);
        this.transkeyword = resultSet.getString(14);
        this.traceId = resultSet.getString(15);
    }

    @Override
    public String toString() {
        return "QueryRecordBean{" +
                "QueryId='" + QueryId + '\'' +
                ", TransCode='" + TransCode + '\'' +
                ", TransInfo='" + TransInfo + '\'' +
                ", QueryAccount='" + QueryAccount + '\'' +
                ", WSStatus='" + WSStatus + '\'' +
                ", WSDetail='" + WSDetail + '\'' +
                ", ErrMsg='" + ErrMsg + '\'' +
                ", DataSource='" + DataSource + '\'' +
                ", relaDSTrans='" + relaDSTrans + '\'' +
                ", retDataNum='" + retDataNum + '\'' +
                ", RequestAddr='" + RequestAddr + '\'' +
                ", Begintime='" + Begintime + '\'' +
                ", Endtime='" + Endtime + '\'' +
                ", transkeyword='" + transkeyword + '\'' +
                ", traceId='" + traceId + '\'' +
                '}';
    }

    @Override
    public int compareTo(QueryRecordBean o) {

        return o.Begintime.compareTo(this.Begintime) ;
    }
}
