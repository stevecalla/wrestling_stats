const fs=require("fs")
const path=require("path")

const log_file=path.join(__dirname,"system_metrics.log")

if(fs.existsSync(log_file)){
  console.log(fs.readFileSync(log_file,"utf8").slice(-4000))
}