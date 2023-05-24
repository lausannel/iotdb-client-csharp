/**
 * Autogenerated by Thrift Compiler (0.14.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;

using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;


#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE1006  // parts of the code use IDL spelling


public partial class TSetTTLReq : TBase
{

  public List<string> StorageGroupPathPattern { get; set; }

  public long TTL { get; set; }

  public TSetTTLReq()
  {
  }

  public TSetTTLReq(List<string> storageGroupPathPattern, long TTL) : this()
  {
    this.StorageGroupPathPattern = storageGroupPathPattern;
    this.TTL = TTL;
  }

  public TSetTTLReq DeepCopy()
  {
    var tmp36 = new TSetTTLReq();
    if((StorageGroupPathPattern != null))
    {
      tmp36.StorageGroupPathPattern = this.StorageGroupPathPattern.DeepCopy();
    }
    tmp36.TTL = this.TTL;
    return tmp36;
  }

  public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
  {
    iprot.IncrementRecursionDepth();
    try
    {
      bool isset_storageGroupPathPattern = false;
      bool isset_TTL = false;
      TField field;
      await iprot.ReadStructBeginAsync(cancellationToken);
      while (true)
      {
        field = await iprot.ReadFieldBeginAsync(cancellationToken);
        if (field.Type == TType.Stop)
        {
          break;
        }

        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.List)
            {
              {
                TList _list37 = await iprot.ReadListBeginAsync(cancellationToken);
                StorageGroupPathPattern = new List<string>(_list37.Count);
                for(int _i38 = 0; _i38 < _list37.Count; ++_i38)
                {
                  string _elem39;
                  _elem39 = await iprot.ReadStringAsync(cancellationToken);
                  StorageGroupPathPattern.Add(_elem39);
                }
                await iprot.ReadListEndAsync(cancellationToken);
              }
              isset_storageGroupPathPattern = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 2:
            if (field.Type == TType.I64)
            {
              TTL = await iprot.ReadI64Async(cancellationToken);
              isset_TTL = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          default: 
            await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            break;
        }

        await iprot.ReadFieldEndAsync(cancellationToken);
      }

      await iprot.ReadStructEndAsync(cancellationToken);
      if (!isset_storageGroupPathPattern)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_TTL)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
    }
    finally
    {
      iprot.DecrementRecursionDepth();
    }
  }

  public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
  {
    oprot.IncrementRecursionDepth();
    try
    {
      var struc = new TStruct("TSetTTLReq");
      await oprot.WriteStructBeginAsync(struc, cancellationToken);
      var field = new TField();
      if((StorageGroupPathPattern != null))
      {
        field.Name = "storageGroupPathPattern";
        field.Type = TType.List;
        field.ID = 1;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        {
          await oprot.WriteListBeginAsync(new TList(TType.String, StorageGroupPathPattern.Count), cancellationToken);
          foreach (string _iter40 in StorageGroupPathPattern)
          {
            await oprot.WriteStringAsync(_iter40, cancellationToken);
          }
          await oprot.WriteListEndAsync(cancellationToken);
        }
        await oprot.WriteFieldEndAsync(cancellationToken);
      }
      field.Name = "TTL";
      field.Type = TType.I64;
      field.ID = 2;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteI64Async(TTL, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      await oprot.WriteFieldStopAsync(cancellationToken);
      await oprot.WriteStructEndAsync(cancellationToken);
    }
    finally
    {
      oprot.DecrementRecursionDepth();
    }
  }

  public override bool Equals(object that)
  {
    if (!(that is TSetTTLReq other)) return false;
    if (ReferenceEquals(this, other)) return true;
    return TCollections.Equals(StorageGroupPathPattern, other.StorageGroupPathPattern)
      && System.Object.Equals(TTL, other.TTL);
  }

  public override int GetHashCode() {
    int hashcode = 157;
    unchecked {
      if((StorageGroupPathPattern != null))
      {
        hashcode = (hashcode * 397) + TCollections.GetHashCode(StorageGroupPathPattern);
      }
      hashcode = (hashcode * 397) + TTL.GetHashCode();
    }
    return hashcode;
  }

  public override string ToString()
  {
    var sb = new StringBuilder("TSetTTLReq(");
    if((StorageGroupPathPattern != null))
    {
      sb.Append(", StorageGroupPathPattern: ");
      StorageGroupPathPattern.ToString(sb);
    }
    sb.Append(", TTL: ");
    TTL.ToString(sb);
    sb.Append(')');
    return sb.ToString();
  }
}

