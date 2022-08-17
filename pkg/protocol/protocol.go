// Package protocol implements a limited COSEM parser which is able to parse output data from the Aidon power meter.
//
// Relevant documentation:
//
// https://github.com/gskjold/AmsToMqttBridge/tree/master/doc/Norway
// https://www.dlms.com/files/Blue_Book_Edition_13-Excerpt.pdf
// https://www.hjemmeautomasjon.no/forums/topic/5032-enda-en-han-til-mqtt-dekoder/
// https://github.com/bmork/obinsect/blob/master/obinsectd.c

package protocol

import (
	`encoding/binary`
	`fmt`
	`io`
)

func ParseString(r io.Reader) (string, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	strlen := int(buf[0])
	buf = make([]byte, strlen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func ParseCode(r io.Reader) (string, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	strlen := int(buf[0])
	buf = make([]byte, strlen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	if strlen != 6 {
		return "", fmt.Errorf("not a code")
	}
	return fmt.Sprintf("%d-%d:%d.%d.%d.%d", buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]), nil
}

func ParseArray(r io.Reader) (any, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	le := int(buf[0])
	arr := make([]any, le)
	for i := 0; i < le; i++ {
		arr[i], err = ParseAny(r)
		if err != nil {
			return arr, err
		}
	}
	return arr, nil
}

func ParseUint8(r io.Reader) (any, error) {
	var i uint8
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseUint16(r io.Reader) (any, error) {
	var i uint16
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseUint32(r io.Reader) (any, error) {
	var i uint32
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseInt8(r io.Reader) (any, error) {
	var i int8
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseInt16(r io.Reader) (any, error) {
	var i int16
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseInt32(r io.Reader) (any, error) {
	var i int32
	err := binary.Read(r, binary.BigEndian, &i)
	return i, err
}

func ParseEnum(r io.Reader) (any, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case 27:
		return "W", nil
	case 28:
		return "VA", nil
	case 29:
		return "VAr", nil
	case 30:
		return "Wh", nil // guessed based on received values
	case 32:
		return "VArh", nil // guessed based on received values
	case 33:
		return "A", nil
	case 35:
		return "V", nil
	default:
		return "", fmt.Errorf("unknown enum index %d", buf[0])
	}
}

func ParseAny(r io.Reader) (any, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	switch buf[0] {
	case 0: // null
		return nil, nil
	case 1: // array
		fallthrough
	case 2: // structure
		return ParseArray(r)
	case 9: // OBIS code
		return ParseCode(r)
	case 10, 12: // string/utf-8
		return ParseString(r)
	case 15: // int
		return ParseInt8(r)
	case 16: // long
		return ParseInt16(r)
	case 17: // unsigned int
		return ParseUint8(r)
	case 18: // unsigned long
		return ParseUint16(r)
	case 5: // double
		return ParseInt32(r)
	case 6: // unsigned double
		return ParseUint32(r)
	case 22: // enum
		return ParseEnum(r)
	default:
		return nil, fmt.Errorf("unrecognized datatype: %d", buf[0])
	}
}

// Parses structured data into a flattened map.
// Only works for this particular data format.
//
// This input data:
//     [
//        "1-0:32.7.0.255",
//        2500,
//        [
//           255,
//           35
//        ]
//     ]
//
// Gives the following output data:
//     {
//        "1-0:32.7.0.255": 2500,
//     }
func ParseFlattened(r io.Reader) (map[string]any, error) {
	result := make(map[string]any)

	data, err := ParseAny(r)
	if err != nil {
		return nil, err
	}
	arr, ok := data.([]any)
	if !ok {
		return nil, fmt.Errorf("top-level structure not of array type")
	}

	for _, item := range arr {
		subarr, ok := item.([]any)
		if !ok {
			return nil, fmt.Errorf("sub-level data not of array type")
		}
		if len(subarr) < 2 {
			return nil, fmt.Errorf("sub-level data does not contain at least two entries")
		}
		key, ok := subarr[0].(string)
		if !ok {
			return nil, fmt.Errorf("first entry not string type; unusable as key")
		}
		result[key] = subarr[1]
	}

	return result, nil
}
