const getISODate = () => {
    return new Date().toISOString()
}

const randomString = (length) => {
    var result = '';
    var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var charactersLength = characters.length;
    for (var i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() *
            charactersLength));
    }
    return result;
}

const isCheckString = (string) => {
    try {
        if (typeof string == 'string') {
            return true;
        } else {
            return false;
        }
    } catch (e) {
        return false;
    }
}

const isJSON = (str) => {
    try {
        return (JSON.parse(str) && !!str);
    } catch (e) {
        return false;
    }
}

const stringuuiid = (string) => {
    try {
        return encrypt(string.toLowerCase().replace(" ", ""))
    } catch (err) {
        return string;
    }
}

const IsCheckDevTenant = (TenantName) => {
    const is_dev_connection = (TenantName.search("-dev") >= 0) ? true : false;
    return is_dev_connection;
}

module.exports = {
    getISODate,
    randomString,
    isCheckString,
    isJSON,
    stringuuiid,
    IsCheckDevTenant
}