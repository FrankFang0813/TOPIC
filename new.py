from multiprocessing import Pool
import crowler104

# 6001020000 花蓮

def main():
    arealist = [str(i) for i in range(6001019001, 6001019016)]  # 台東區域

    # str(i)
    # for i in range(6001008001, 6001008029)]  # 台中市區域
    # for i in range(6001019001, 6001019016):  # 台東區域
    #     arealist.append(str(i))
    # for i in range(6001011001, 6001011013):  # 南投區域
    #     arealist.append(str(i))
    #arealist = ['6001001001']  #test
    paramlist = []
    print("paramslist making ")
    for area in arealist:
        paramlist += crowler104.make_params(sceng=0, area=area, min=0, max=37999) # params(salary0~37999,所有地區)
        paramlist += crowler104.make_params(sceng=1, area=area, min=38000, max=40000)
        paramlist += crowler104.make_params(sceng=0, area=area, min=40001, max='')
    print("paramslist done",paramlist)
    print("paramslist length:",len(paramlist))
    print("getting index.....")

    index = []
    # for i in paramlist:

    for i in paramlist:
        try:
            index += crowler104.index(i)
        except :
            print(i)

    indexjson = {'index': index} if index != [] or len(index)<10 else False
    print('all done,total index amount:{}'.format(len(indexjson['index'])))
    crowler104.dump_json_file(indexjson)






if __name__ == '__main__':

    main()