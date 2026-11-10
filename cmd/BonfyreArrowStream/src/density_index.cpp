// A read-only, columnar projection of two native authorities: RepositoryFabric
// source-cut receipts and CMS PackGraph BFP1 images. Parquet is a derived index,
// never the source of an installation, a source grant, or an upstream Git claim.
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <CommonCrypto/CommonDigest.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "../../BonfyreArrowFlight/src/sha256.h"

namespace {
constexpr size_t kMaxInputs = 16384, kMaxBfp = 262184, kMaxCut = 8192;
constexpr size_t kStrings = 17;
enum Field {
  Kind, Identity, Owner, CutDigest, ImageDigest, ClosureDigest, Pack,
  Provider, Org, Repo, Ref, Filter, Commit, Tree, SourcePath, Evidence, Link
};
struct Row { std::array<std::string,kStrings> field; int64_t bytes=0; };
bool Hex(std::string_view v, size_t n) {
  if (v.size()!=n) return false;
  for(char c:v) if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')))return false;
  return true;
}
std::string Sha(std::string_view body) {
  unsigned char h[CC_SHA256_DIGEST_LENGTH];
  CC_SHA256(body.data(),static_cast<CC_LONG>(body.size()),h);
  static constexpr char chars[]="0123456789abcdef";
  std::string out(64,'0');
  for(size_t i=0;i<32;i++){out[2*i]=chars[h[i]>>4];out[2*i+1]=chars[h[i]&15];}
  return out;
}
bool ReadPinned(const std::string& path,size_t cap,std::string& body) {
  struct stat before,after;
  if(lstat(path.c_str(),&before)||!S_ISREG(before.st_mode)||before.st_size<0||
     static_cast<uint64_t>(before.st_size)>cap)return false;
  int flags=O_RDONLY;
#ifdef O_NOFOLLOW
  flags|=O_NOFOLLOW;
#endif
  int fd=open(path.c_str(),flags);
  if(fd<0)return false;
  if(fstat(fd,&after)||!S_ISREG(after.st_mode)||after.st_dev!=before.st_dev||
     after.st_ino!=before.st_ino||after.st_size!=before.st_size){close(fd);return false;}
  body.resize(static_cast<size_t>(before.st_size));
  size_t done=0;
  while(done<body.size()) {
    ssize_t n=read(fd,body.data()+done,body.size()-done);
    if(n<=0){close(fd);return false;}done+=static_cast<size_t>(n);
  }
  if(fstat(fd,&after)||after.st_size!=before.st_size||after.st_mtime!=before.st_mtime){
    close(fd);return false;
  }
  close(fd);return true;
}
bool Decimal(std::string_view raw,size_t& at,size_t& number) {
  size_t end=raw.find(':',at);
  if(end==raw.npos||end==at||end-at>10)return false;
  size_t parsed=0;
  auto result=std::from_chars(raw.data()+at,raw.data()+end,parsed);
  if(result.ec!=std::errc{}||result.ptr!=raw.data()+end)return false;
  at=end+1;number=parsed;return true;
}
bool CutReceipt(const std::string& path,Row& row) {
  std::string body;if(!ReadPinned(path,kMaxCut,body))return false;
  std::map<std::string,std::string> fields;size_t start=0;
  while(start<body.size()) {
    const size_t end=body.find('\n',start);
    const size_t stop=end==std::string::npos?body.size():end;
    if(stop>start) {
      const auto colon=body.find(": ",start);
      if(colon==std::string::npos||colon>=stop)return false;
      std::string key=body.substr(start,colon-start),value=body.substr(colon+2,stop-colon-2);
      if(key.empty()||value.size()>400||!fields.emplace(key,value).second)return false;
    }
    if(end==std::string::npos)break;
    start=end+1;
  }
  const char* needed[]={"schema","source_provider","source_org","source_repository",
    "requested_ref","josh_filter","virtual_commit","virtual_tree","cut_digest",
    "canonical_mapping","pack_root","install_authority","publication_authority"};
  for(const char* key:needed)if(!fields.contains(key))return false;
  if(fields["schema"]!="bonfyre.repository-cut.v1"||
     fields["pack_root"]!="uncompiled"||fields["install_authority"]!="none"||
     fields["publication_authority"]!="none"||
     !Hex(fields["cut_digest"],64)||
     !(Hex(fields["virtual_commit"],40)||Hex(fields["virtual_commit"],64))||
     !(Hex(fields["virtual_tree"],40)||Hex(fields["virtual_tree"],64)))return false;
  const auto& filter=fields["josh_filter"];
  if(filter!="none"&&(filter.empty()||filter[0]!=':'))return false;
  if(fields["canonical_mapping"]!=(filter=="none"?"direct-git":"unverified"))return false;
  const std::string binding=std::string("bonfyre.repository-cut.v1\n")+
    fields["source_provider"]+'\n'+fields["source_org"]+'\n'+fields["source_repository"]+'\n'+
    fields["requested_ref"]+'\n'+(filter=="none"?"":filter)+'\n'+
    fields["virtual_commit"]+'\n'+fields["virtual_tree"]+'\n';
  if(Sha(binding)!=fields["cut_digest"])return false;
  row.field[Kind]="repository_cut";
  row.field[Identity]=row.field[CutDigest]=fields["cut_digest"];
  row.field[Owner]="BonfyreRepositoryFabric";
  row.field[Provider]=fields["source_provider"];
  row.field[Org]=fields["source_org"];row.field[Repo]=fields["source_repository"];
  row.field[Ref]=fields["requested_ref"];row.field[Filter]=filter;
  row.field[Commit]=fields["virtual_commit"];row.field[Tree]=fields["virtual_tree"];
  row.field[SourcePath]=path;
  row.field[Evidence]="receipt-self-consistent";
  row.field[Link]=fields["canonical_mapping"];
  row.bytes=static_cast<int64_t>(body.size());
  return true;
}
// The CMS closure is already typed and length-prefixed. Parse its actual
// demand rows rather than searching free text for repository-cut strings.
bool Closure(std::string_view raw,std::string& pack,std::set<std::string>& cuts) {
  constexpr std::string_view prefix="pack-closure-v1:";
  if(!raw.starts_with(prefix))return false;
  size_t at=prefix.size(),length=0;
  if(!Decimal(raw,at,length)||length==0||length>=96||length>raw.size()-at)return false;
  pack=std::string(raw.substr(at,length));at+=length;
  static constexpr std::string_view valid="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:";
  if(pack.find_first_not_of(valid)!=std::string::npos)return false;
  while(at<raw.size()) {
    size_t tagLength=0;if(!Decimal(raw,at,tagLength)||tagLength==0||tagLength>12||
      tagLength>raw.size()-at)return false;
    const auto tag=raw.substr(at,tagLength);at+=tagLength;
    int fields=tag=="artifact"?9:tag=="version"?6:tag=="dependency"?4:
      tag=="overlay"?7:tag=="demand"?7:0;
    if(!fields)return false;
    while(at<raw.size()&&raw[at]=='R') {
      at++;
      std::vector<std::string_view> values;
      values.reserve(static_cast<size_t>(fields));
      for(int field=0;field<fields;field++) {
        size_t type=0,size=0;
        if(!Decimal(raw,at,type)||type>5||!Decimal(raw,at,size)||size>16384||
           size>raw.size()-at)return false;
        values.push_back(raw.substr(at,size));at+=size;
      }
      if(tag=="demand"&&values[0]=="source") {
        constexpr std::string_view marker="repository-cut:";
        const auto selected=values[2];
        if(selected.starts_with(marker)) {
          const auto digest=selected.substr(marker.size());
          if(!Hex(digest,64))return false;
          cuts.emplace(digest);
        }
      }
    }
  }
  return true;
}
bool PackImage(const std::string& path,std::vector<Row>& output) {
  std::string bytes;if(!ReadPinned(path,kMaxBfp,bytes)||bytes.size()<40||
     bytes.compare(0,4,"BFP1"))return false;
  uint32_t length=0;
  for(int i=4;i<8;i++)length=(length<<8)|static_cast<unsigned char>(bytes[i]);
  if(static_cast<size_t>(length)!=bytes.size()-40)return false;
  const std::string_view closure(bytes.data()+40,length);
  std::string closureDigest=Sha(closure);
  std::string headerDigest;
  static constexpr char chars[]="0123456789abcdef";
  for(int i=8;i<40;i++){
    unsigned char c=static_cast<unsigned char>(bytes[i]);
    headerDigest+=chars[c>>4];headerDigest+=chars[c&15];
  }
  if(headerDigest!=closureDigest)return false;
  std::string pack;std::set<std::string> cuts;
  if(!Closure(closure,pack,cuts))return false;
  const std::string imageDigest=Sha(bytes);
  Row image;
  image.field[Kind]="pack_image";image.field[Identity]=image.field[ImageDigest]=imageDigest;
  image.field[Owner]="BonfyreCMS";image.field[Pack]=pack;
  image.field[ClosureDigest]=closureDigest;image.field[SourcePath]=path;
  image.field[Evidence]="binary-and-closure-hash-valid";
  image.field[Link]="install-unknown";image.bytes=static_cast<int64_t>(bytes.size());
  output.push_back(std::move(image));
  for(const auto& cut:cuts) {
    Row edge;
    edge.field[Kind]="pack_source_cut";
    edge.field[Identity]=Sha("bonfyre.pack-source-cut.v1\n"+imageDigest+'\n'+cut+'\n');
    edge.field[Owner]="BonfyreCMS";
    edge.field[CutDigest]=cut;edge.field[ImageDigest]=imageDigest;
    edge.field[ClosureDigest]=closureDigest;edge.field[Pack]=pack;
    edge.field[SourcePath]=path;edge.field[Evidence]="declared-in-closure-only";
    edge.field[Link]="receipt-not-required-for-declaration";
    output.push_back(std::move(edge));
  }
  return true;
}
bool Scan(const std::string& directory,const std::string& extension,std::vector<std::string>& paths){
  namespace fs=std::filesystem;std::error_code error;
  if(!fs::is_directory(fs::symlink_status(directory,error))||error)return false;
  for(const auto& entry:fs::directory_iterator(directory,fs::directory_options::none,error)) {
    if(error)return false;
    if(entry.path().extension()!=extension)continue;
    if(entry.is_symlink(error)||!entry.is_regular_file(error)||error)return false;
    paths.push_back(entry.path().string());
    if(paths.size()>kMaxInputs)return false;
  }
  return !error;
}
arrow::Status WriteTable(const std::string& path,const std::vector<Row>& rows,std::string& digest) {
  constexpr std::array<const char*,kStrings> names={
    "kind","identity_digest","owner","source_cut_digest","pack_image_digest",
    "closure_digest","pack","repository_provider","repository_org","repository_name",
    "git_ref","josh_filter","git_commit","git_tree","source_path","evidence_state","link_state"};
  std::array<arrow::StringBuilder,kStrings> builders;
  arrow::Int64Builder sizeBuilder;
  for(const Row& row:rows){
    for(size_t i=0;i<kStrings;i++)ARROW_RETURN_NOT_OK(builders[i].Append(row.field[i]));
    ARROW_RETURN_NOT_OK(sizeBuilder.Append(row.bytes));
  }
  std::vector<std::shared_ptr<arrow::Array>> columns;
  std::vector<std::shared_ptr<arrow::Field>> schemaFields;
  for(size_t i=0;i<kStrings;i++){
    std::shared_ptr<arrow::Array> out;
    ARROW_RETURN_NOT_OK(builders[i].Finish(&out));
    columns.push_back(std::move(out));
    schemaFields.push_back(arrow::field(names[i],arrow::utf8(),false));
  }
  std::shared_ptr<arrow::Array> sizes;
  ARROW_RETURN_NOT_OK(sizeBuilder.Finish(&sizes));columns.push_back(std::move(sizes));
  schemaFields.push_back(arrow::field("source_bytes",arrow::int64(),false));
  auto table=arrow::Table::Make(arrow::schema(schemaFields),columns);
  int flags=O_WRONLY|O_CREAT|O_EXCL;
#ifdef O_NOFOLLOW
  flags|=O_NOFOLLOW;
#endif
  int fd=open(path.c_str(),flags,0600);
  if(fd<0)return arrow::Status::IOError("Parquet target must be a new, exclusive regular file");
  auto opened=arrow::io::FileOutputStream::Open(fd);
  if(!opened.ok()){close(fd);unlink(path.c_str());return opened.status();}
  auto sink=*opened;
  parquet::WriterProperties::Builder properties;
  properties.compression(parquet::Compression::ZSTD);
  const auto written=parquet::arrow::WriteTable(*table,arrow::default_memory_pool(),sink,2048,properties.build());
  const auto closed=sink->Close();
  if(!written.ok()||!closed.ok()){unlink(path.c_str());return written.ok()?closed:written;}
  parquet::arrow::FileReaderBuilder readerBuilder;
  auto status=readerBuilder.OpenFile(path);
  if(!status.ok()){unlink(path.c_str());return status;}
  auto built=readerBuilder.Build();
  if(!built.ok()){unlink(path.c_str());return built.status();}
  if((*built)->parquet_reader()->metadata()->num_rows()!=static_cast<int64_t>(rows.size())){
    unlink(path.c_str());return arrow::Status::Invalid("Parquet row count mismatch");
  }
  auto file=arrow::io::ReadableFile::Open(path);
  if(!file.ok()){unlink(path.c_str());return file.status();}
  auto hash=bonfyre_arrow::Sha256File(*file);
  if(!hash.ok()){unlink(path.c_str());return hash.status();}
  digest=*hash;return arrow::Status::OK();
}
} // namespace

int bonfyre_density_index(int argc,char** argv) {
  if(argc==3&&std::string_view(argv[1])=="index"&&
     (std::string_view(argv[2])=="--help"||std::string_view(argv[2])=="-h"||std::string_view(argv[2])=="help")){
    std::cout<<"usage: bonfyre parquet index [--cut FILE] [--cuts DIR] [--pack-image FILE] [--packs DIR] --out NEW.parquet\n"
             <<"  Parquet is a read-only derived index of verified source-cut receipts, BFP1 images, and declared edges\n";
    return 0;
  }
  std::vector<std::string> cuts,packs;
  std::string output;
  for(int i=2;i<argc;i++){
    std::string_view opt(argv[i]);
    if(i+1>=argc){std::cerr<<"parquet index: option needs a value\n";return 2;}
    const std::string value=argv[++i];
    if(opt=="--cut")cuts.push_back(value);
    else if(opt=="--pack-image")packs.push_back(value);
    else if(opt=="--cuts") {if(!Scan(value,".yaff",cuts)){std::cerr<<"cannot scan source-cut directory\n";return 2;}}
    else if(opt=="--packs") {if(!Scan(value,".bfp",packs)){std::cerr<<"cannot scan pack-image directory\n";return 2;}}
    else if(opt=="--out") {if(!output.empty()){std::cerr<<"duplicate output\n";return 2;}output=value;}
    else {std::cerr<<"unknown Parquet index option\n";return 2;}
    if(cuts.size()+packs.size()>kMaxInputs){std::cerr<<"input partition too large\n";return 2;}
  }
  if(output.empty()||output[0]!='/'||output.size()>2048||output.find('\n')!=std::string::npos||
     (cuts.empty()&&packs.empty())){
    std::cerr<<"usage: bonfyre parquet index --cut FILE|--cuts DIR --pack-image FILE|--packs DIR --out NEW.parquet\n";
    return 2;
  }
  std::sort(cuts.begin(),cuts.end());cuts.erase(std::unique(cuts.begin(),cuts.end()),cuts.end());
  std::sort(packs.begin(),packs.end());packs.erase(std::unique(packs.begin(),packs.end()),packs.end());
  std::vector<Row> rows;
  std::set<std::string> observed;
  for(const auto& path:cuts){
    Row row;if(!CutReceipt(path,row)){std::cerr<<"source-cut receipt refused: "<<path<<'\n';return 1;}
    observed.insert(row.field[Identity]);rows.push_back(std::move(row));
  }
  for(const auto& path:packs){
    if(!PackImage(path,rows)){std::cerr<<"pack BFP1 refused: "<<path<<'\n';return 1;}
  }
  if(rows.size()>kMaxInputs*8){std::cerr<<"derived edge count exceeds bound\n";return 2;}
  for(Row& row:rows){
    if(row.field[Kind]=="pack_source_cut"){
      bool matched=observed.contains(row.field[CutDigest]);
      row.field[Link]=matched?"receipt-matched":"declaration-only";
    }
  }
  std::sort(rows.begin(),rows.end(),[](const Row& a,const Row& b){
    if(a.field[Kind]!=b.field[Kind])return a.field[Kind]<b.field[Kind];
    if(a.field[Identity]!=b.field[Identity])return a.field[Identity]<b.field[Identity];
    return a.field[SourcePath]<b.field[SourcePath];
  });
  // One immutable index row per semantic identity even if multiple folders
  // contain the same receipt or content-addressed image.
  rows.erase(std::unique(rows.begin(),rows.end(),[](const Row&a,const Row&b){
    return a.field[Kind]==b.field[Kind]&&a.field[Identity]==b.field[Identity];
  }),rows.end());
  const size_t linked=static_cast<size_t>(std::count_if(rows.begin(),rows.end(),[](const Row& row){
    return row.field[Kind]=="pack_source_cut"&&row.field[Link]=="receipt-matched";
  }));
  const size_t cutCount=static_cast<size_t>(std::count_if(rows.begin(),rows.end(),[](const Row& row){
    return row.field[Kind]=="repository_cut";
  }));
  const size_t packCount=static_cast<size_t>(std::count_if(rows.begin(),rows.end(),[](const Row& row){
    return row.field[Kind]=="pack_image";
  }));
  std::string parquetDigest;
  const auto status=WriteTable(output,rows,parquetDigest);
  if(!status.ok()){std::cerr<<"Parquet index refused: "<<status.ToString()<<'\n';return 1;}
  std::cout<<"schema\tbonfyre.density-index.v1\nstate\tmaterialized\n"
           <<"representation\tparquet-zstd\nrows\t"<<rows.size()<<'\n'
           <<"source_cuts\t"<<cutCount<<'\n'<<"pack_images\t"<<packCount<<'\n'
           <<"matched_edges\t"<<linked<<'\n'<<"sha256\t"<<parquetDigest<<'\n'
           <<"path\t"<<output<<'\n'
           <<"authority\tderived-only\n";
  return 0;
}
