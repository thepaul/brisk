//HELPER FUNCTIONS AND STRUCTURES

PortfolioMgr_get_leaderboard_args = function(args){
}
PortfolioMgr_get_leaderboard_args.prototype = {}
PortfolioMgr_get_leaderboard_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

PortfolioMgr_get_leaderboard_args.prototype.write = function(output){ 
output.writeStructBegin('PortfolioMgr_get_leaderboard_args')
output.writeFieldStop()
output.writeStructEnd()
return
}

PortfolioMgr_get_leaderboard_result = function(args){
this.success = new LeaderBoard()
if( args != null ){if (null != args.success)
this.success = args.success
}}
PortfolioMgr_get_leaderboard_result.prototype = {}
PortfolioMgr_get_leaderboard_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.STRUCT) {
this.success = new LeaderBoard()
this.success.read(input)
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

PortfolioMgr_get_leaderboard_result.prototype.write = function(output){ 
output.writeStructBegin('PortfolioMgr_get_leaderboard_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.STRUCT, 0)
this.success.write(output)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

PortfolioMgr_get_portfolios_args = function(args){
this.start_token = null
this.limit = null
if( args != null ){if (null != args.start_token)
this.start_token = args.start_token
if (null != args.limit)
this.limit = args.limit
}}
PortfolioMgr_get_portfolios_args.prototype = {}
PortfolioMgr_get_portfolios_args.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 1:if (ftype == Thrift.Type.STRING) {
var rtmp = input.readString()
this.start_token = rtmp.value
} else {
  input.skip(ftype)
}
break
case 2:if (ftype == Thrift.Type.I32) {
var rtmp = input.readI32()
this.limit = rtmp.value
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

PortfolioMgr_get_portfolios_args.prototype.write = function(output){ 
output.writeStructBegin('PortfolioMgr_get_portfolios_args')
if (null != this.start_token) {
output.writeFieldBegin('start_token', Thrift.Type.STRING, 1)
output.writeString(this.start_token)
output.writeFieldEnd()
}
if (null != this.limit) {
output.writeFieldBegin('limit', Thrift.Type.I32, 2)
output.writeI32(this.limit)
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

PortfolioMgr_get_portfolios_result = function(args){
this.success = null
if( args != null ){if (null != args.success)
this.success = args.success
}}
PortfolioMgr_get_portfolios_result.prototype = {}
PortfolioMgr_get_portfolios_result.prototype.read = function(input){ 
var ret = input.readStructBegin()
while (1) 
{
var ret = input.readFieldBegin()
var fname = ret.fname
var ftype = ret.ftype
var fid   = ret.fid
if (ftype == Thrift.Type.STOP) 
break
switch(fid)
{
case 0:if (ftype == Thrift.Type.LIST) {
{
var _size21 = 0
var rtmp3
this.success = []
var _etype24 = 0
rtmp3 = input.readListBegin()
_etype24 = rtmp3.etype
_size21 = rtmp3.size
for (var _i25 = 0; _i25 < _size21; ++_i25)
{
var elem26 = null
elem26 = new Portfolio()
elem26.read(input)
this.success.push(elem26)
}
input.readListEnd()
}
} else {
  input.skip(ftype)
}
break
default:
  input.skip(ftype)
}
input.readFieldEnd()
}
input.readStructEnd()
return
}

PortfolioMgr_get_portfolios_result.prototype.write = function(output){ 
output.writeStructBegin('PortfolioMgr_get_portfolios_result')
if (null != this.success) {
output.writeFieldBegin('success', Thrift.Type.LIST, 0)
{
output.writeListBegin(Thrift.Type.STRUCT, this.success.length)
{
for(var iter27 in this.success)
{
iter27=this.success[iter27]
iter27.write(output)
}
}
output.writeListEnd()
}
output.writeFieldEnd()
}
output.writeFieldStop()
output.writeStructEnd()
return
}

PortfolioMgrClient = function(input, output) {
  this.input  = input
  this.output = null == output ? input : output
  this.seqid  = 0
}
PortfolioMgrClient.prototype = {}
PortfolioMgrClient.prototype.get_leaderboard = function(){
this.send_get_leaderboard()
return this.recv_get_leaderboard()
}

PortfolioMgrClient.prototype.send_get_leaderboard = function(){
this.output.writeMessageBegin('get_leaderboard', Thrift.MessageType.CALL, this.seqid)
var args = new PortfolioMgr_get_leaderboard_args()
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

PortfolioMgrClient.prototype.recv_get_leaderboard = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new PortfolioMgr_get_leaderboard_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "get_leaderboard failed: unknown result"
}
PortfolioMgrClient.prototype.get_portfolios = function(start_token,limit){
this.send_get_portfolios(start_token, limit)
return this.recv_get_portfolios()
}

PortfolioMgrClient.prototype.send_get_portfolios = function(start_token,limit){
this.output.writeMessageBegin('get_portfolios', Thrift.MessageType.CALL, this.seqid)
var args = new PortfolioMgr_get_portfolios_args()
args.start_token = start_token
args.limit = limit
args.write(this.output)
this.output.writeMessageEnd()
return this.output.getTransport().flush()
}

PortfolioMgrClient.prototype.recv_get_portfolios = function(){
var ret = this.input.readMessageBegin()
var fname = ret.fname
var mtype = ret.mtype
var rseqid= ret.rseqid
if (mtype == Thrift.MessageType.EXCEPTION) {
  var x = new Thrift.ApplicationException()
  x.read(this.input)
  this.input.readMessageEnd()
  throw x
}
var result = new PortfolioMgr_get_portfolios_result()
result.read(this.input)
this.input.readMessageEnd()

if (null != result.success ) {
  return result.success
}
throw "get_portfolios failed: unknown result"
}
