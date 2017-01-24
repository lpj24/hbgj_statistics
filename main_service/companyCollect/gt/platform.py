def update_platform_focus_by_file():
    from collections import defaultdict
    import ast
    focus_file = open("focus.dat", "a")
    every_day_data = defaultdict(dict)

    with open("/home/huolibi/code/cal2017/cal0118_hbdt_focus/data/hbdt_focus_platform.dat") as hbdt_focus_data:

        for hbdt_data in hbdt_focus_data:

            (userid, phoneid, phone, token, flyid, focusdate, flydate
             , createtime, platform, ordertype) = hbdt_data.strip().split("\t")

            if ast.literal_eval(phoneid) is None:
                if platform == "jieji":
                    phone_id = token
                else:
                    continue
            else:
                phone_id = phoneid if int(phoneid) > 0 else userid

            create_time = createtime.split(" ")[0] if createtime else focusdate.split(" ")[0]

            if platform in ['iphone', 'android'] and ordertype == '0' and userid.find('gt') < 0:
                platform = platform
            elif platform.lower() == 'iphonepro' and ordertype == '0' and userid.find('gt') < 0:
                platform = "iphone"
            elif platform == 'weixin' and ordertype == '0':
                platform = 'weixin'
            elif platform == 'jieji' and ordertype == '0':
                platform = "jieji"
                phone_id = token
            elif platform == 'gtgj' and ordertype == '0' and userid.find('gt') >= 0:
                platform = 'gtgj'

            try:
                (every_day_data[create_time])[platform].append(phone_id)
            except KeyError:
                (every_day_data[create_time])[platform] = [phone_id]

    with open("/home/huolibi/code/cal2017/cal0118_hbdt_focus/data/hbdt_focus_platform_his.dat") as hbdt_focus_data_his:

        for hbdt_data in hbdt_focus_data_his:
            try:
                (userid, phoneid, phone, token, flyid, focusdate, flydate, createtime, platform, ordertype) \
                    = hbdt_data.strip().split("\t")
            except Exception:
                continue

            if ast.literal_eval(phoneid) is None:
                if platform == "jieji":
                    phone_id = token
                else:
                    continue
            else:
                phone_id = phoneid if int(phoneid) > 0 else userid

            create_time = createtime.split(" ")[0] if createtime else focusdate.split(" ")[0]

            if platform in ['iphone', 'android'] and ordertype == '0' and userid.find('gt') < 0:
                platform = platform
            elif platform.lower() == 'iphonepro' and ordertype == '0' and userid.find('gt') < 0:
                platform = "iphone"
            elif platform == 'weixin' and ordertype == '0':
                platform = 'weixin'
            elif platform == 'jieji' and ordertype == '0':
                platform = "jieji"
                phone_id = token
            elif platform == 'gtgj' and ordertype == '0' and userid.find('gt') >= 0:
                platform = 'gtgj'

            try:
                (every_day_data[create_time])[platform].append(phone_id)
            except KeyError:
                (every_day_data[create_time])[platform] = [phone_id]

    for k, v in every_day_data.items():
        s_day = k
        total_phone = []
        android_uv = iphone_uv = gtgj_uv = weixin_uv = jieji_uv = total_uv = 0
        android_pv = iphone_pv = gtgj_pv = weixin_pv = jieji_pv = total_pv = 0

        for platform_k, platform_v in v.items():
            total_phone.extend(platform_v)
            if platform_k == "android":
                android_uv = len(set(platform_v))
                android_pv = len(platform_v)
            if platform_k == "iphone":
                iphone_uv = len(set(platform_v))
                iphone_pv = len(platform_v)
            if platform_k == "gtgj":
                gtgj_uv = len(set(platform_v))
                gtgj_pv = len(platform_v)
            if platform_k == "weixin":
                weixin_uv = len(set(platform_v))
                weixin_pv = len(platform_v)
            if platform_k == "jieji":
                jieji_uv = len(set(platform_v))
                jieji_pv = len(platform_v)
        total_uv = len(set(total_phone))
        total_pv = len(total_phone)

        if s_day == '2015-10-16':
            print total_phone
        out_str = s_day + "\t" + str(android_uv) + "\t" + str(iphone_uv) + "\t" + str(weixin_uv) + "\t" \
                  + str(gtgj_uv) + "\t" + str(jieji_uv) + "\t" + "0" + "\t" + str(total_uv) + "\t" + str(android_pv) + "\t" + \
                  str(iphone_pv) + "\t" + str(weixin_pv) + "\t" + str(gtgj_pv) + "\t" + str(jieji_pv) + "\t" + "0" + "\t" + \
                  str(total_pv)
        focus_file.write(out_str + "\n")

    focus_file.close()

if __name__ == "__main__":
    update_platform_focus_by_file()