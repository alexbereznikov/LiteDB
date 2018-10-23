﻿using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using static LiteDB.Constants;

namespace LiteDB.Engine
{
    /// <summary>
    /// Read multiple array segment as a single linear segment - Fordward Only
    /// NO ThreadSafe
    /// </summary>
    public class BufferReader : IDisposable
    {
        private readonly IEnumerator<ArraySegment<byte>> _source;
        private readonly bool _utcDate;

        private ArraySegment<byte> _current;
        private int _currentPosition = 0; // position in _current
        private int _position = 0; // global position

        private bool _isEOF = false;

        private byte[] _tempBuffer = new byte[16]; // re-usable array

        /// <summary>
        /// Current global cursor position
        /// </summary>
        public int Position => _position;

        /// <summary>
        /// Indicate position are at end of last source array segment
        /// </summary>
        public bool IsEOF => _isEOF;

        public BufferReader(IEnumerable<ArraySegment<byte>> source, bool utcDate = false)
        {
            _source = source.GetEnumerator();
            _utcDate = utcDate;

            _source.MoveNext();
            _current = _source.Current;
        }

        #region Basic Read

        /// <summary>
        /// Move fordward in current segment. If array segment finish, open next segment
        /// Returns true if move to another segment - returns false if continue in same segment
        /// </summary>
        private bool MoveFordward(int count)
        {
            // do not move fordward if source finish
            if (_isEOF) return false;

            //DEBUG
            if (_currentPosition + count > _current.Count) throw new InvalidOperationException("fordward are only for current segment");

            _currentPosition += count;
            _position += count;

            // request new source array if _current all consumed
            if (_currentPosition == _current.Count)
            {
                if (_source.MoveNext() == false)
                {
                    _isEOF = true;
                }
                else
                {
                    _current = _source.Current;
                    _currentPosition = 0;
                }

                return true;
            }

            return false;
        }

        /// <summary>
        /// Read bytes from source and copy into buffer. Return how many bytes was read
        /// </summary>
        public int Read(byte[] buffer, int offset, int count)
        {
            var bufferPosition = 0;

            while (bufferPosition < count)
            {
                var bytesLeft = _current.Count - _currentPosition;
                var bytesToCopy = Math.Min(count - bufferPosition, bytesLeft);

                // fill buffer
                if (buffer != null)
                {
                    Buffer.BlockCopy(_current.Array, 
                        _current.Offset + _currentPosition, 
                        buffer, 
                        offset + bufferPosition, 
                        bytesToCopy);
                }

                bufferPosition += bytesToCopy;

                // move position in current segment (and go to next segment if finish)
                this.MoveFordward(bytesToCopy);

                if (_isEOF) break;
            }

            return bufferPosition;
        }

        /// <summary>
        /// Skip bytes (same as Read but with no array copy)
        /// </summary>
        public int Skip(int count) => this.Read(null, 0, count);

        #endregion

        #region Read String

        /// <summary>
        /// Read string with fixed size
        /// </summary>
        public string ReadString(int count)
        {
            string value;

            // if fits in current segment, use inner array - otherwise copy from multiples segments
            if (_currentPosition + count <= _current.Count)
            {
                value = Encoding.UTF8.GetString(_current.Array, _current.Offset + _currentPosition, count);

                this.MoveFordward(count);
            }
            else
            {
                // rent a buffer to be re-usable
                var buffer = ArrayPool<byte>.Shared.Rent(count);

                this.Read(buffer, 0, count);

                value = Encoding.UTF8.GetString(buffer, 0, count);

                ArrayPool<byte>.Shared.Return(buffer);
            }

            return value;
        }

        /// <summary>
        /// Reading string until find \0 at end
        /// </summary>
        public string ReadCString()
        {
            if (this.TryReadCStringCurrentSegment(out var value))
            {
                return value;
            }
            else
            {
                using (var mem = new MemoryStream())
                {
                    // copy all first segment 
                    var initialCount = _current.Count - _currentPosition;

                    mem.Write(_current.Array, _current.Offset + _currentPosition, initialCount);

                    this.MoveFordward(initialCount);

                    // and go to next segment
                    if (!_isEOF)
                    {
                        while (_current.Get(_currentPosition) != 0x00)
                        {
                            if (this.MoveFordward(1))
                            {
                                // write all segment into strem (did not found \0 yet)
                                mem.Write(_current.Array, _current.Offset, _current.Count);
                            }

                            if (_isEOF) break;
                        }

                        // add last segment (if eof already added in while)
                        if (!_isEOF)
                        {
                            mem.Write(_current.Array, _current.Offset, _currentPosition);
                        }

                        this.MoveFordward(1); // +1 to '\0'
                    }

                    return Encoding.UTF8.GetString(mem.ToArray());
                }
            }
        }

        /// <summary>
        /// Try read CString in current segment avoind read byte-to-byte over segments
        /// </summary>
        private bool TryReadCStringCurrentSegment(out string value)
        {
            var pos = _currentPosition;
            var count = 0;

            while(pos < _current.Count)
            {
                if (_current.Get(pos) == 0x00)
                {
                    value = Encoding.UTF8.GetString(_current.Array, _current.Offset + _currentPosition, count);

                    this.MoveFordward(count + 1); // +1 means '\0'

                    return true;
                }
                else
                {
                    count++;
                    pos++;
                }
            }

            value = null;

            return false;
        }

        #endregion

        #region Read Numbers

        private T ReadNumber<T>(Func<byte[], int, T> convert, int size)
        {
            T value;

            // if fits in current segment, use inner array - otherwise copy from multiples segments
            if (_currentPosition + size <= _current.Count)
            {
                value = convert(_current.Array, _current.Offset + _currentPosition);

                this.MoveFordward(size);
            }
            else
            {
                this.Read(_tempBuffer, 0, size);

                value = convert(_tempBuffer, 0);
            }

            return value;
        }

        public Int16 ReadInt16() => this.ReadNumber(BitConverter.ToInt16, 2);
        public Int32 ReadInt32() => this.ReadNumber(BitConverter.ToInt32, 4);
        public Int64 ReadInt64() => this.ReadNumber(BitConverter.ToInt64, 8);
        public UInt16 ReadUInt16() => this.ReadNumber(BitConverter.ToUInt16, 2);
        public UInt32 ReadUInt32() => this.ReadNumber(BitConverter.ToUInt32, 4);
        public UInt64 ReadUInt64() => this.ReadNumber(BitConverter.ToUInt64, 8);
        public Single ReadSingle() => this.ReadNumber(BitConverter.ToSingle, 4);
        public Double ReadDouble() => this.ReadNumber(BitConverter.ToDouble, 8);

        public Decimal ReadDecimal()
        {
            var a = this.ReadInt32();
            var b = this.ReadInt32();
            var c = this.ReadInt32();
            var d = this.ReadInt32();
            return new Decimal(new int[] { a, b, c, d });
        }

        #endregion

        #region Complex Types

        /// <summary>
        /// Read DateTime as UTC ticks (not BSON format)
        /// </summary>
        public DateTime ReadDateTime()
        {
            var date = new DateTime(this.ReadInt64(), DateTimeKind.Utc);

            return _utcDate ? date.ToLocalTime() : date;
        }

        /// <summary>
        /// Read Guid as 16 bytes array
        /// </summary>
        public Guid ReadGuid()
        {
            return new Guid(this.ReadBytes(16));
        }

        /// <summary>
        /// Write ObjectId as 12 bytes array
        /// </summary>
        public ObjectId ReadObjectId()
        {
            ObjectId value;

            if (_currentPosition + 12 <= _current.Count)
            {
                value = new ObjectId(_current.Array, _current.Offset + _currentPosition);

                this.MoveFordward(12);
            }
            else
            {
                this.Read(_tempBuffer, 0, 12);

                value = new ObjectId(_tempBuffer, 0);
            }

            return value;
        }

        /// <summary>
        /// Write a boolean as 1 byte (0 or 1)
        /// </summary>
        public bool ReadBoolean()
        {
            var value = _current.Get(_currentPosition) == 1;
            this.MoveFordward(1);
            return value;
        }

        /// <summary>
        /// Write single byte
        /// </summary>
        public byte ReadByte()
        {
            var value = _current.Get(_currentPosition);
            this.MoveFordward(1);
            return value;
        }

        /// <summary>
        /// Write PageAddress as PageID, Index
        /// </summary>
        internal PageAddress ReadPageAddress()
        {
            return new PageAddress(this.ReadUInt32(), this.ReadUInt16());
        }

        /// <summary>
        /// Read byte array - not great because need create new array instance
        /// </summary>
        public byte[] ReadBytes(int count)
        {
            var buffer = new byte[count];
            this.Read(buffer, 0, count);
            return buffer;
        }

        #endregion

        #region BsonValue for IndexKey

        /// <summary>
        /// Read a BSON value from buffer. Do not respect BSON document specs, becase read single value
        /// Need know length for variable types (string|binary)
        /// If buffer contains a BsonArray or BsonDocument will write full BSON spcecs (using Elements)
        /// Used ONLY Index Key storage
        /// </summary>
        public BsonValue ReadBsonValue(ushort length)
        {
            var type = (BsonType)this.ReadByte();

            switch (type)
            {
                case BsonType.Null: return BsonValue.Null;

                case BsonType.Int32: return this.ReadInt32();
                case BsonType.Int64: return this.ReadInt64();
                case BsonType.Double: return this.ReadDouble();
                case BsonType.Decimal: return this.ReadDecimal();

                case BsonType.String: return this.ReadString(length);

                case BsonType.Document: return this.ReadDocument();
                case BsonType.Array: return this.ReadArray();

                case BsonType.Binary: return this.ReadBytes(length);
                case BsonType.ObjectId: return this.ReadObjectId();
                case BsonType.Guid: return this.ReadGuid();

                case BsonType.Boolean: return this.ReadBoolean();
                case BsonType.DateTime: return this.ReadDateTime();

                case BsonType.MinValue: return BsonValue.MinValue;
                case BsonType.MaxValue: return BsonValue.MaxValue;

                default: throw new NotImplementedException();
            }
        }

        #endregion

        #region BsonDocument as SPECS

        /// <summary>
        /// Read a BsonDocument from reader
        /// </summary>
        public BsonDocument ReadDocument(HashSet<string> fields = null)
        {
            var length = this.ReadInt32();
            var end = _position + length - 5;
            var remaining = fields == null || fields.Count == 0 ? null : new HashSet<string>(fields, StringComparer.OrdinalIgnoreCase);

            DEBUG(remaining != null && remaining.Contains("$"), "if contains $, should be be empty/full hash");

            var doc = new BsonDocument();

            while (_position < end && (remaining == null || remaining?.Count > 0))
            {
                var value = this.ReadElement(remaining, out string name);

                // null value means are not selected field
                if (value != null)
                {
                    doc.RawValue[name] = value;

                    // remove from remaining fields
                    remaining?.Remove(name);
                }
            }

            this.MoveFordward(1); // skip \0

            return doc;
        }

        /// <summary>
        /// Read an BsonArray from reader
        /// </summary>
        public BsonArray ReadArray()
        {
            var length = this.ReadInt32();
            var end = _position + length - 5;
            var arr = new BsonArray();

            while (_position < end)
            {
                var value = this.ReadElement(null, out string name);
                arr.Add(value);
            }

            this.MoveFordward(1); // skip \0

            return arr;
        }

        /// <summary>
        /// Reads an element (key-value) from an reader
        /// </summary>
        private BsonValue ReadElement(HashSet<string> remaining, out string name)
        {
            var type = this.ReadByte();
            name = this.ReadCString();

            // check if need skip this element
            if (remaining != null && !remaining.Contains(name))
            {
                // define skip length according type
                var length =
                    (type == 0x0A || type == 0xFF || type == 0x7F) ? 0 : // Null, MinValue, MaxValue
                    (type == 0x08) ? 1 : // Boolean
                    (type == 0x10) ? 4 : // Int
                    (type == 0x01 || type == 0x12 || type == 0x09) ? 8 : // Double, Int64, DateTime
                    (type == 0x07) ? 12 : // ObjectId
                    (type == 0x13) ? 16 : // Decimal
                    (type == 0x02) ? this.ReadInt32() : // String
                    (type == 0x05) ? this.ReadInt32() + 1 : // Binary (+1 for subtype)
                    (type == 0x03 || type == 0x04) ? this.ReadInt32() - 4 : 0; // Document, Array (-4 to Length + zero)

                if (length > 0)
                {
                    this.Skip(length);
                }

                return null;
            }

            if (type == 0x01) // Double
            {
                return this.ReadDouble();
            }
            else if (type == 0x02) // String
            {
                var length = this.ReadInt32();
                var value = this.ReadString(length);
                this.MoveFordward(1); // read '\0'
                return value;
            }
            else if (type == 0x03) // Document
            {
                return this.ReadDocument();
            }
            else if (type == 0x04) // Array
            {
                return this.ReadArray();
            }
            else if (type == 0x05) // Binary
            {
                var length = this.ReadInt32();
                var subType = this.ReadByte();
                var bytes = this.ReadBytes(length);

                switch (subType)
                {
                    case 0x00: return bytes;
                    case 0x04: return new Guid(bytes);
                }
            }
            else if (type == 0x07) // ObjectId
            {
                return this.ReadObjectId();
            }
            else if (type == 0x08) // Boolean
            {
                return this.ReadBoolean();
            }
            else if (type == 0x09) // DateTime
            {
                var ts = this.ReadInt64();

                // catch specific values for MaxValue / MinValue #19
                if (ts == 253402300800000) return DateTime.MaxValue;
                if (ts == -62135596800000) return DateTime.MinValue;

                var date = BsonValue.UnixEpoch.AddMilliseconds(ts);

                return _utcDate ? date : date.ToLocalTime();
            }
            else if (type == 0x0A) // Null
            {
                return BsonValue.Null;
            }
            else if (type == 0x10) // Int32
            {
                return this.ReadInt32();
            }
            else if (type == 0x12) // Int64
            {
                return this.ReadInt64();
            }
            else if (type == 0x13) // Decimal
            {
                return this.ReadDecimal();
            }
            else if (type == 0xFF) // MinKey
            {
                return BsonValue.MinValue;
            }
            else if (type == 0x7F) // MaxKey
            {
                return BsonValue.MaxValue;
            }

            throw new NotSupportedException("BSON type not supported");
        }

        #endregion

        public void Dispose()
        {
            _source.Dispose();
        }
    }
}