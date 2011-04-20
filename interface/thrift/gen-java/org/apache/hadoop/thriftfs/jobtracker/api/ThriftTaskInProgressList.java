/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.thriftfs.jobtracker.api;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

/**
 * Container structure of a list of tasks. This list may have been put together
 * according to some selection criteria. That is, it may not correspond to the
 * mapTasks, or reduceTasks, etc. It may even contain tasks of different types.
 */
public class ThriftTaskInProgressList implements TBase<ThriftTaskInProgressList, ThriftTaskInProgressList._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("ThriftTaskInProgressList");

  private static final TField TASKS_FIELD_DESC = new TField("tasks", TType.LIST, (short)1);
  private static final TField NUM_TOTAL_TASKS_FIELD_DESC = new TField("numTotalTasks", TType.I32, (short)2);

  /**
   * A (possibly incomplete) list of tasks
   */
  public List<ThriftTaskInProgress> tasks;
  /**
   * The total number of tasks in this full list.
   */
  public int numTotalTasks;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    /**
     * A (possibly incomplete) list of tasks
     */
    TASKS((short)1, "tasks"),
    /**
     * The total number of tasks in this full list.
     */
    NUM_TOTAL_TASKS((short)2, "numTotalTasks");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TASKS
          return TASKS;
        case 2: // NUM_TOTAL_TASKS
          return NUM_TOTAL_TASKS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __NUMTOTALTASKS_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TASKS, new FieldMetaData("tasks", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, ThriftTaskInProgress.class))));
    tmpMap.put(_Fields.NUM_TOTAL_TASKS, new FieldMetaData("numTotalTasks", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(ThriftTaskInProgressList.class, metaDataMap);
  }

  public ThriftTaskInProgressList() {
  }

  public ThriftTaskInProgressList(
    List<ThriftTaskInProgress> tasks,
    int numTotalTasks)
  {
    this();
    this.tasks = tasks;
    this.numTotalTasks = numTotalTasks;
    setNumTotalTasksIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ThriftTaskInProgressList(ThriftTaskInProgressList other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetTasks()) {
      List<ThriftTaskInProgress> __this__tasks = new ArrayList<ThriftTaskInProgress>();
      for (ThriftTaskInProgress other_element : other.tasks) {
        __this__tasks.add(new ThriftTaskInProgress(other_element));
      }
      this.tasks = __this__tasks;
    }
    this.numTotalTasks = other.numTotalTasks;
  }

  public ThriftTaskInProgressList deepCopy() {
    return new ThriftTaskInProgressList(this);
  }

  @Override
  public void clear() {
    this.tasks = null;
    setNumTotalTasksIsSet(false);
    this.numTotalTasks = 0;
  }

  public int getTasksSize() {
    return (this.tasks == null) ? 0 : this.tasks.size();
  }

  public java.util.Iterator<ThriftTaskInProgress> getTasksIterator() {
    return (this.tasks == null) ? null : this.tasks.iterator();
  }

  public void addToTasks(ThriftTaskInProgress elem) {
    if (this.tasks == null) {
      this.tasks = new ArrayList<ThriftTaskInProgress>();
    }
    this.tasks.add(elem);
  }

  /**
   * A (possibly incomplete) list of tasks
   */
  public List<ThriftTaskInProgress> getTasks() {
    return this.tasks;
  }

  /**
   * A (possibly incomplete) list of tasks
   */
  public ThriftTaskInProgressList setTasks(List<ThriftTaskInProgress> tasks) {
    this.tasks = tasks;
    return this;
  }

  public void unsetTasks() {
    this.tasks = null;
  }

  /** Returns true if field tasks is set (has been asigned a value) and false otherwise */
  public boolean isSetTasks() {
    return this.tasks != null;
  }

  public void setTasksIsSet(boolean value) {
    if (!value) {
      this.tasks = null;
    }
  }

  /**
   * The total number of tasks in this full list.
   */
  public int getNumTotalTasks() {
    return this.numTotalTasks;
  }

  /**
   * The total number of tasks in this full list.
   */
  public ThriftTaskInProgressList setNumTotalTasks(int numTotalTasks) {
    this.numTotalTasks = numTotalTasks;
    setNumTotalTasksIsSet(true);
    return this;
  }

  public void unsetNumTotalTasks() {
    __isset_bit_vector.clear(__NUMTOTALTASKS_ISSET_ID);
  }

  /** Returns true if field numTotalTasks is set (has been asigned a value) and false otherwise */
  public boolean isSetNumTotalTasks() {
    return __isset_bit_vector.get(__NUMTOTALTASKS_ISSET_ID);
  }

  public void setNumTotalTasksIsSet(boolean value) {
    __isset_bit_vector.set(__NUMTOTALTASKS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TASKS:
      if (value == null) {
        unsetTasks();
      } else {
        setTasks((List<ThriftTaskInProgress>)value);
      }
      break;

    case NUM_TOTAL_TASKS:
      if (value == null) {
        unsetNumTotalTasks();
      } else {
        setNumTotalTasks((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TASKS:
      return getTasks();

    case NUM_TOTAL_TASKS:
      return new Integer(getNumTotalTasks());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TASKS:
      return isSetTasks();
    case NUM_TOTAL_TASKS:
      return isSetNumTotalTasks();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ThriftTaskInProgressList)
      return this.equals((ThriftTaskInProgressList)that);
    return false;
  }

  public boolean equals(ThriftTaskInProgressList that) {
    if (that == null)
      return false;

    boolean this_present_tasks = true && this.isSetTasks();
    boolean that_present_tasks = true && that.isSetTasks();
    if (this_present_tasks || that_present_tasks) {
      if (!(this_present_tasks && that_present_tasks))
        return false;
      if (!this.tasks.equals(that.tasks))
        return false;
    }

    boolean this_present_numTotalTasks = true;
    boolean that_present_numTotalTasks = true;
    if (this_present_numTotalTasks || that_present_numTotalTasks) {
      if (!(this_present_numTotalTasks && that_present_numTotalTasks))
        return false;
      if (this.numTotalTasks != that.numTotalTasks)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_tasks = true && (isSetTasks());
    builder.append(present_tasks);
    if (present_tasks)
      builder.append(tasks);

    boolean present_numTotalTasks = true;
    builder.append(present_numTotalTasks);
    if (present_numTotalTasks)
      builder.append(numTotalTasks);

    return builder.toHashCode();
  }

  public int compareTo(ThriftTaskInProgressList other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    ThriftTaskInProgressList typedOther = (ThriftTaskInProgressList)other;

    lastComparison = Boolean.valueOf(isSetTasks()).compareTo(typedOther.isSetTasks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTasks()) {
      lastComparison = TBaseHelper.compareTo(this.tasks, typedOther.tasks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumTotalTasks()).compareTo(typedOther.isSetNumTotalTasks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumTotalTasks()) {
      lastComparison = TBaseHelper.compareTo(this.numTotalTasks, typedOther.numTotalTasks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // TASKS
          if (field.type == TType.LIST) {
            {
              TList _list43 = iprot.readListBegin();
              this.tasks = new ArrayList<ThriftTaskInProgress>(_list43.size);
              for (int _i44 = 0; _i44 < _list43.size; ++_i44)
              {
                ThriftTaskInProgress _elem45;
                _elem45 = new ThriftTaskInProgress();
                _elem45.read(iprot);
                this.tasks.add(_elem45);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // NUM_TOTAL_TASKS
          if (field.type == TType.I32) {
            this.numTotalTasks = iprot.readI32();
            setNumTotalTasksIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.tasks != null) {
      oprot.writeFieldBegin(TASKS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.tasks.size()));
        for (ThriftTaskInProgress _iter46 : this.tasks)
        {
          _iter46.write(oprot);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(NUM_TOTAL_TASKS_FIELD_DESC);
    oprot.writeI32(this.numTotalTasks);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftTaskInProgressList(");
    boolean first = true;

    sb.append("tasks:");
    if (this.tasks == null) {
      sb.append("null");
    } else {
      sb.append(this.tasks);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("numTotalTasks:");
    sb.append(this.numTotalTasks);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}

