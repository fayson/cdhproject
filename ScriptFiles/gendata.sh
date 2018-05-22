function rand(){  
    min=$1  
    max=$(($2-$min+1))  
    num=$(($RANDOM+1000000000))
    echo $(($num%$max+$min))  
}  
  

let i=1
while [ $i -le 3 ];
do
 let n=1
 while [ $n -le $1 ];
 do
  let month=$n%12+1
  if [ $month -eq 2 ];then
    let day=$n%28+1
  else
    let day=$n%30+1
  fi  

  let hour=$n%24
  rnd=$(rand 10000 10100) 
  echo "$i$n,$i$n,$i$n,2017-$month-$day $hour:20:00,${rnd},$n,$n,$n" >> data$i.txt
  let n=n+1
 done

let i=i+1
done
