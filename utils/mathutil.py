import math
def cdf(z):  # p-value
    return  (1.0 + math.erf(z / math.sqrt(2.0))) / 2.0

def laplace_cdf(z):
    return 0.5 * (1 + math.copysign(1, z) * (1 - math.exp(math.copysign(z, -1) * math.sqrt(2))))
