SND_TABLE = [
    0.5000, 0.5398, 0.5793, 0.6179, 0.6554, 0.6915, 0.7257, 0.7580, 0.7881, 0.8159,
    0.8413, 0.8643, 0.8849, 0.9032, 0.9192, 0.9332, 0.9452, 0.9554, 0.9641, 0.9713,
    0.9772, 0.9821, 0.9861, 0.9893, 0.9918, 0.9938, 0.9953, 0.9965, 0.9974, 0.9981
]
def cdf(z):  # p-value
    _z = z if z > 0 else -z
    index = int( _z * 10)
    if index >= 29:
        if _z == 2.9:
            return SND_TABLE[29]
        return 0.9999
    subindex = (_z * 10 - index)
    return SND_TABLE[index] * (1-subindex) + SND_TABLE[index+1] * subindex

    