#PBS -l nodes=1:ppn=4,walltime=3:00:00
#PBS -N poli_13
#PBS -e errors/error_pol_13.log
#PBS -o out/out_pol_13.log
#PBS -M ryan.sampana@mail.mcgill.ca
#PBS -m abe
module load iomkl/2015b Python/3.5.0
cd ~/polisci
source ./venv3/bin/activate
cd ~/polisci/sa_polling/
python make_13.py > out_13.log
