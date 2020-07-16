def R1_periods(lastperiod):
    assert isinstance(lastperiod, int) == True
    if len(str(lastperiod)) != 6:
        raise ValueError("Must have 6 digit for Year and Month YYYYMM format.")
    if str(lastperiod)[-2:] == '12':
        return (lastperiod-300,lastperiod-200,lastperiod-100,lastperiod)
    if str(lastperiod)[-2:] != '12':
        return (int(str(int(str(lastperiod)[:4])-3)+'12'),
            int(str(int(str(lastperiod)[:4])-2) +'12'),
            int(str(int(str(lastperiod)[:4])-1) + str(lastperiod)[-2:]),
            int(str(int(str(lastperiod)[:4])-1) +'12'),
            lastperiod)

if __name__ == '__main__':
    print(R1_periods(201907))
    print(R1_periods(201812))
